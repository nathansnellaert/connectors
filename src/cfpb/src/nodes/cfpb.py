"""CFPB download node — a heterogeneous, two-API source.

Entities (entity union: complaints, hmda_filers, hmda_loans):

  - complaints  : CFPB Consumer Complaint Database (mechanism ``ccdb_rest``).
                  ~15.5M rows / ~8GB CSV, updates daily. The Elasticsearch
                  backend caps offset pagination at frm+size <= 10000, so the
                  full corpus is walked by ``date_received`` windows using the
                  CSV export (``&format=csv``), which is NOT subject to that
                  cap (a one-week window already returns ~66k rows). Modelled
                  as a record-stream firehose: one streamed CSV->parquet batch
                  per received-month (``cfpb-complaints-YYYY-MM``). State holds
                  the latest month fetched; recent months are re-pulled each
                  refresh so per-complaint field updates (company_response,
                  timely, consumer_disputed) are captured.

  - hmda_filers : HMDA filing-institution roster (``hmda_data_browser_rest``).
                  Small JSON per year (~5k institutions/year). Full re-pull of
                  every available year each run -> one parquet (~35k rows).

  - hmda_loans  : HMDA nationwide loan-level disclosures. ~6GB CSV per year,
                  ~88 columns. Firehose: one streamed gzip-CSV batch per year
                  (``cfpb-hmda-loans-YYYY``). State tracks completed years; the
                  latest available year is always re-pulled (annual data is
                  revised), and a per-run year cap spreads the backfill.

Notes from probing (2026-05-30):
  - CCDB returns 403 to the default User-Agent; a browser-style UA is required.
  - HMDA year availability is discovered by probing the ``filers`` endpoint,
    which returns HTTP 400 for any year whose data is not yet published
    (e.g. 2017 and 2025 today), so there is no hardcoded upper year bound.
  - CCDB complaint narratives embed newlines/commas -> the CSV parser needs
    ``newlines_in_values=True``.
"""
import time
from datetime import datetime, timezone

import httpx
import pyarrow as pa
import pyarrow.csv as pacsv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import (
    NodeSpec,
    get,
    get_client,
    load_state,
    raw_parquet_writer,
    raw_writer,
    save_raw_parquet,
    save_state,
)

STATE_VERSION = 1

_UA = "Mozilla/5.0 (compatible; subsets-data-bot/1.0; +https://subsets.io)"
_JSON_HEADERS = {"User-Agent": _UA, "Accept": "application/json"}
_CSV_HEADERS = {"User-Agent": _UA, "Accept": "text/csv"}

CCDB_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
HMDA_BASE = "https://ffiec.cfpb.gov/v2/data-browser-api/"

# Earliest CCDB date_received, live-probed 2026-05-30.
CCDB_MIN_MONTH = (2011, 12)
# HMDA modernized dataset floor; the API rejects years < 2018 (verified 2026-05-30).
HMDA_MIN_YEAR = 2018

# Re-pull this many trailing months each refresh so complaint field updates
# (status changes after receipt) are captured; transform dedups by complaint_id.
OVERLAP_MONTHS = 2
# Per-run wall-clock budget for the complaints firehose. Hitting it returns
# cleanly with the watermark advanced; the next refresh resumes.
MAX_COMPLAINTS_SECONDS = 1500
# One ~6GB year of HMDA loans per run; the next refresh takes the next year.
MAX_LOAN_YEARS_PER_RUN = 1

# Exact CSV header of the CCDB export (live-probed 2026-05-30). Stored as
# strings — the export is text and dates use MM/DD/YY; transform re-types.
COMPLAINT_COLUMNS = [
    "Date received",
    "Product",
    "Sub-product",
    "Issue",
    "Sub-issue",
    "Consumer complaint narrative",
    "Company public response",
    "Company",
    "State",
    "ZIP code",
    "Tags",
    "Consumer consent provided?",
    "Submitted via",
    "Date sent to company",
    "Company response to consumer",
    "Timely response?",
    "Consumer disputed?",
    "Complaint ID",
]
COMPLAINT_SCHEMA = pa.schema([(c, pa.string()) for c in COMPLAINT_COLUMNS])

FILER_SCHEMA = pa.schema([
    ("lei", pa.string()),
    ("name", pa.string()),
    ("count", pa.int64()),
    ("period", pa.int64()),
])

# --------------------------------------------------------------------------- #
# Transport
# --------------------------------------------------------------------------- #

_TRANSIENT_EXC = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
    httpx.ProxyError,
)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _get_json(url: str, params: dict) -> dict:
    resp = get(url, params=params, headers=_JSON_HEADERS, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


class _StreamReader:
    """Minimal file-like wrapper over an httpx byte iterator.

    Lets ``pyarrow.csv.open_csv`` parse a multi-GB CSV response without
    buffering the whole body in memory.
    """

    def __init__(self, byte_iter):
        self._it = byte_iter
        self._buf = b""
        self._done = False
        self.closed = False

    def readable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            chunks = [self._buf]
            self._buf = b""
            chunks.extend(self._it)
            self._done = True
            return b"".join(chunks)
        while len(self._buf) < n and not self._done:
            try:
                self._buf += next(self._it)
            except StopIteration:
                self._done = True
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def close(self) -> None:
        self.closed = True


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(4),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _stream_csv_to_parquet(url: str, params: dict, asset: str, schema: pa.Schema) -> int:
    """Stream a CSV response and write it as parquet (all columns string).

    Retrying re-opens the parquet writer from scratch (truncate), so a
    transient mid-stream failure is idempotent.
    """
    read_opts = pacsv.ReadOptions(block_size=8 << 20)
    parse_opts = pacsv.ParseOptions(newlines_in_values=True)
    conv_opts = pacsv.ConvertOptions(
        column_types={f.name: pa.string() for f in schema},
        strings_can_be_null=False,
    )
    client = get_client()
    n_rows = 0
    timeout = httpx.Timeout(connect=15.0, read=300.0, write=60.0, pool=15.0)
    with client.stream("GET", url, params=params, headers=_CSV_HEADERS, timeout=timeout) as resp:
        resp.raise_for_status()
        reader = _StreamReader(resp.iter_bytes())
        stream = pacsv.open_csv(
            reader, read_options=read_opts, parse_options=parse_opts, convert_options=conv_opts
        )
        with raw_parquet_writer(asset, schema) as writer:
            for batch in stream:
                if batch.num_rows == 0:
                    continue
                # from_batches with explicit schema validates column set/order;
                # any CCDB schema drift fails loudly here rather than silently.
                writer.write_table(pa.Table.from_batches([batch], schema=schema))
                n_rows += batch.num_rows
    return n_rows


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(4),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _stream_to_csv_gz(url: str, params: dict, asset: str) -> int:
    """Stream a (multi-GB) CSV response straight to a gzip-compressed raw file.

    Retrying re-opens the writer (truncate), so a transient mid-download
    failure is idempotent.
    """
    client = get_client()
    n_bytes = 0
    timeout = httpx.Timeout(connect=15.0, read=600.0, write=60.0, pool=15.0)
    with client.stream("GET", url, params=params, headers=_CSV_HEADERS, timeout=timeout) as resp:
        resp.raise_for_status()
        with raw_writer(asset, "csv.gz", mode="wb", compression="gzip") as f:
            for chunk in resp.iter_bytes(chunk_size=1 << 20):
                f.write(chunk)
                n_bytes += len(chunk)
                if n_bytes % (256 << 20) < (1 << 20):
                    print(f"[{asset}] streamed ~{n_bytes >> 20} MiB", flush=True)
    return n_bytes


# --------------------------------------------------------------------------- #
# Month helpers (complaints firehose)
# --------------------------------------------------------------------------- #


def _month_add(ym: tuple, k: int) -> tuple:
    idx = (ym[0] * 12 + (ym[1] - 1)) + k
    return (idx // 12, idx % 12 + 1)


def _month_range(start: tuple, end: tuple):
    cur = start
    while cur <= end:
        yield cur
        cur = _month_add(cur, 1)


# --------------------------------------------------------------------------- #
# HMDA year discovery
# --------------------------------------------------------------------------- #


def _discover_hmda_years() -> list:
    """Return the available HMDA years by probing the filers endpoint.

    An unpublished year returns HTTP 400 (permanent), so it is skipped; a
    published year returns 200 with a non-empty institutions list.
    """
    now = datetime.now(timezone.utc)
    years = []
    for y in range(HMDA_MIN_YEAR, now.year + 1):
        try:
            payload = _get_json(HMDA_BASE + "view/filers", {"years": y})
        except httpx.HTTPStatusError as exc:
            if 400 <= exc.response.status_code < 500 and exc.response.status_code != 429:
                continue  # year not available yet
            raise
        if isinstance(payload, dict) and payload.get("institutions"):
            years.append(y)
    return years


# --------------------------------------------------------------------------- #
# Fetch functions
# --------------------------------------------------------------------------- #


def fetch_complaints(node_id: str) -> None:
    state_key = node_id
    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        state = {}

    watermark = state.get("watermark")  # "YYYY-MM" of the latest month fetched
    now = datetime.now(timezone.utc)
    cur_month = (now.year, now.month)

    if watermark:
        wy, wm = (int(p) for p in watermark.split("-"))
        start = _month_add((wy, wm), -OVERLAP_MONTHS)
        if start < CCDB_MIN_MONTH:
            start = CCDB_MIN_MONTH
    else:
        start = CCDB_MIN_MONTH

    deadline = time.monotonic() + MAX_COMPLAINTS_SECONDS
    max_done = watermark
    total = 0

    for ym in _month_range(start, cur_month):
        if time.monotonic() > deadline:
            print(f"[complaints] per-run budget reached at {ym[0]}-{ym[1]:02d}", flush=True)
            break
        label = f"{ym[0]:04d}-{ym[1]:02d}"
        asset = f"cfpb-complaints-{label}"
        nxt = _month_add(ym, 1)
        params = {
            "date_received_min": f"{ym[0]:04d}-{ym[1]:02d}-01",
            "date_received_max": f"{nxt[0]:04d}-{nxt[1]:02d}-01",
            "format": "csv",
            "size": 5_000_000,
        }
        # Raw first, then state — a crash loses at most this month, never
        # creates a phantom completion.
        n_rows = _stream_csv_to_parquet(CCDB_URL, params, asset, COMPLAINT_SCHEMA)
        total += n_rows
        if max_done is None or label > max_done:
            max_done = label
        save_state(state_key, {
            "schema_version": STATE_VERSION,
            "watermark": max_done,
            "last_success_at": now.isoformat(),
            "last_run_stats": {"records_this_run": total, "last_month": label},
        })
        print(f"[complaints] {label}: {n_rows} rows (run total {total})", flush=True)


def fetch_hmda_filers(node_id: str) -> None:
    asset = node_id
    years = _discover_hmda_years()
    if not years:
        raise RuntimeError("HMDA filers: no available years discovered")

    rows = []
    for y in years:
        payload = _get_json(HMDA_BASE + "view/filers", {"years": y})
        for inst in payload.get("institutions", []):
            rows.append({
                "lei": inst.get("lei"),
                "name": inst.get("name"),
                "count": inst.get("count"),
                "period": inst.get("period", y),
            })

    table = pa.Table.from_pylist(rows, schema=FILER_SCHEMA)
    save_raw_parquet(table, asset)
    print(f"[hmda_filers] {len(rows)} filer-years across {years}", flush=True)


def fetch_hmda_loans(node_id: str) -> None:
    state_key = node_id
    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        state = {}

    done = set(state.get("done_years", []))
    years = _discover_hmda_years()
    if not years:
        raise RuntimeError("HMDA loans: no available years discovered")

    latest = max(years)
    done.discard(latest)  # always refresh the latest year (annual data is revised)
    pending = sorted(y for y in years if y not in done)

    processed = 0
    for y in pending:
        if processed >= MAX_LOAN_YEARS_PER_RUN:
            print(
                f"[hmda_loans] per-run cap reached; {len(pending) - processed} year(s) remain",
                flush=True,
            )
            break
        asset = f"cfpb-hmda-loans-{y}"
        n_bytes = _stream_to_csv_gz(HMDA_BASE + "view/nationwide/csv", {"years": y}, asset)
        done.add(y)
        processed += 1
        save_state(state_key, {
            "schema_version": STATE_VERSION,
            "done_years": sorted(done),
            "last_success_at": datetime.now(timezone.utc).isoformat(),
            "last_run_stats": {"year": y, "bytes": n_bytes},
        })
        print(f"[hmda_loans] year {y}: {n_bytes} bytes streamed", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id="cfpb-complaints", fn=fetch_complaints, kind="download"),
    NodeSpec(id="cfpb-hmda-filers", fn=fetch_hmda_filers, kind="download"),
    NodeSpec(id="cfpb-hmda-loans", fn=fetch_hmda_loans, kind="download"),
]
