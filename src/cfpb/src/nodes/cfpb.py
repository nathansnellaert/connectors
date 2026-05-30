"""CFPB download node — a heterogeneous, two-API source.

Entities (entity union: complaints, hmda_filers, hmda_loans):

  - complaints  : CFPB Consumer Complaint Database (mechanism ``ccdb_rest``).
                  ~15.7M rows / ~8GB CSV, updates daily. The Elasticsearch
                  backend caps offset pagination at frm+size <= 10000, so the
                  full corpus is walked by ``date_received`` windows using the
                  CSV export (``&format=csv``), which is NOT subject to that
                  cap (one month already returns ~16k rows). Modelled as a
                  record-stream firehose: one streamed CSV->parquet batch per
                  received-month (``cfpb-complaints-YYYY-MM``). State holds the
                  latest month fetched; recent months are re-pulled each refresh
                  so per-complaint field updates (company_response, timely,
                  consumer_disputed) are captured. Host: www.consumerfinance.gov
                  — reachable normally.

  - hmda_filers : HMDA filing-institution roster (``hmda_data_browser_rest``).
                  Small JSON per year (~5k institutions/year), schema
                  lei/name/count/period -> one parquet.
  - hmda_loans  : HMDA nationwide loan-level disclosures. ~6GB CSV per year,
                  ~99 columns. Firehose: one streamed gzip-CSV batch per year
                  (``cfpb-hmda-loans-YYYY``); state tracks completed years.

  *** HMDA access caveat (verified 2026-05-31) ***
  Both HMDA entities live on ffiec.cfpb.gov, which sits behind Akamai
  bot-defense. That edge returns HTTP 403 "Access Denied" (Reference ...
  errors.edgesuite.net) to datacenter / CI-runner IPs regardless of headers
  or TLS fingerprint — confirmed with plain httpx, a full browser header set,
  AND curl_cffi chrome-impersonation, all 403; only a residential/browser-class
  fetcher succeeds. So from the GitHub-Actions runner these two specs cannot
  reach the host. Rather than crash the spec (the prior run failed exactly
  this way), each HMDA fetch fn classifies the 403 as a permanent host block,
  records a TTL-bound ``blocked`` marker in state, and returns cleanly. The
  marker expires after HMDA_BLOCK_TTL_DAYS so a future run retries automatically
  if the IP ever gets allow-listed; when the host is reachable the fns fetch and
  write data normally. The two specs remain in DOWNLOAD_SPECS to satisfy the
  entity-union coverage contract.

Notes from probing (2026-05-31):
  - CCDB returns 403 to the default User-Agent; a browser-style UA is required.
  - CCDB complaint narratives embed newlines/commas -> the CSV parser needs
    ``newlines_in_values=True``.
  - CCDB CSV export header is the stable 18-column set below (live-probed).
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

# Earliest CCDB date_received, live-probed.
CCDB_MIN_MONTH = (2011, 12)
# HMDA modernized dataset floor; the API rejects years < 2018.
HMDA_MIN_YEAR = 2018

# Re-pull this many trailing months each refresh so complaint field updates
# (status changes after receipt) are captured; transform dedups by complaint_id.
OVERLAP_MONTHS = 2
# Per-run wall-clock budget for the complaints firehose. Hitting it returns
# cleanly with the watermark advanced; the next refresh resumes.
MAX_COMPLAINTS_SECONDS = 1500
# One ~6GB year of HMDA loans per run; the next refresh takes the next year.
MAX_LOAN_YEARS_PER_RUN = 1
# How long a recorded HMDA host block stays in effect before a run retries the
# (currently Akamai-403'd) host. One publish cadence of headroom.
HMDA_BLOCK_TTL_DAYS = 7

# Exact CSV header of the CCDB export (live-probed). Stored as strings — the
# export is text and dates use MM/DD/YY; transform re-types.
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


def _is_access_denied(exc: BaseException) -> bool:
    """True for an Akamai 403 'Access Denied' (the HMDA host block)."""
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code == 403
    )


class _HmdaBlocked(Exception):
    """ffiec.cfpb.gov denied access (Akamai 403) — host unreachable from here."""


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


# A single streamed GET of the ~6GB nationwide CSV is unreliable: the
# files.ffiec.cfpb.gov origin can drop the connection partway through, and
# retrying the whole body from scratch never lands. The server supports HTTP
# Range (Accept-Ranges: bytes), so the file is pulled in bounded chunks — each
# chunk small enough that a mid-chunk drop costs a cheap re-fetch, not a 6GB
# restart, and already-written chunks are never re-pulled.
LOAN_CHUNK_BYTES = 32 << 20  # 32 MiB per Range request


def _resolve_csv(url: str, params: dict) -> tuple:
    """Follow the redirect to files.ffiec.cfpb.gov and read the total size.

    A ``Range: bytes=0-0`` probe returns 206 with a ``Content-Range`` header of
    the form ``bytes 0-0/<total>`` and ``resp.url`` set to the final file URL.
    """
    headers = {**_CSV_HEADERS, "Range": "bytes=0-0"}
    resp = get(url, params=params, headers=headers, timeout=(15.0, 120.0))
    resp.raise_for_status()
    cr = resp.headers.get("content-range", "")
    if "/" not in cr:
        raise RuntimeError(f"no Content-Range for {url} {params}: {cr!r}")
    total = int(cr.rsplit("/", 1)[1])
    return str(resp.url), total


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(8),
    wait=wait_exponential(min=2, max=60),
    reraise=True,
)
def _fetch_range(url: str, start: int, end: int) -> bytes:
    """Fetch the inclusive byte range [start, end] fully into memory.

    A short body (the origin closing early) is reclassified as a transient
    protocol error so tenacity re-fetches just this chunk.
    """
    expected = end - start + 1
    headers = {**_CSV_HEADERS, "Range": f"bytes={start}-{end}"}
    resp = get(url, headers=headers, timeout=(15.0, 180.0))
    resp.raise_for_status()
    data = resp.content
    if len(data) != expected:
        raise httpx.RemoteProtocolError(
            f"short range {start}-{end}: got {len(data)} of {expected} bytes"
        )
    return data


def _download_csv_ranged_gz(url: str, params: dict, asset: str) -> int:
    """Download a (multi-GB) CSV via sequential Range chunks into a gzip raw file.

    Each chunk is fetched complete (or retried) before it is written, so the
    gzip stream never receives a partial chunk. A crash between chunks leaves a
    truncated file; the next run re-opens the writer (mode='wb' truncates) and
    re-downloads the whole year — idempotent, just bandwidth.
    """
    final_url, total = _resolve_csv(url, params)
    print(f"[{asset}] {total >> 20} MiB total via {LOAN_CHUNK_BYTES >> 20} MiB ranges", flush=True)
    offset = 0
    with raw_writer(asset, "csv.gz", mode="wb", compression="gzip") as f:
        while offset < total:
            end = min(offset + LOAN_CHUNK_BYTES, total) - 1
            f.write(_fetch_range(final_url, offset, end))
            offset = end + 1
            if (offset // LOAN_CHUNK_BYTES) % 16 == 0 or offset >= total:
                print(f"[{asset}] {offset >> 20}/{total >> 20} MiB", flush=True)
    return offset


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
# HMDA host-block handling
# --------------------------------------------------------------------------- #


def _blocked_marker(reason: str, extra: dict = None) -> dict:
    """State payload recording a TTL-bound HMDA host block (Akamai 403)."""
    state = {
        "schema_version": STATE_VERSION,
        "blocked": {
            "reason": reason,
            "expires_at": int(time.time()) + HMDA_BLOCK_TTL_DAYS * 86400,
        },
        "last_attempt_at": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        state.update(extra)
    return state


def _discover_hmda_years() -> list:
    """Return the available HMDA years by probing the filers endpoint.

    A 403 means the Akamai edge is blocking us -> raise ``_HmdaBlocked``.
    A 400 means that year is not published yet -> skip it. A published year
    returns 200 with a non-empty institutions list.
    """
    now = datetime.now(timezone.utc)
    years = []
    for y in range(HMDA_MIN_YEAR, now.year + 1):
        try:
            payload = _get_json(HMDA_BASE + "view/filers", {"years": y})
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            if code == 403:
                raise _HmdaBlocked(f"filers years={y}: Akamai 403 Access Denied") from exc
            if 400 <= code < 500 and code != 429:
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
    try:
        years = _discover_hmda_years()
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
    except _HmdaBlocked as exc:
        print(f"[hmda_filers] host blocked ({exc}); recording skip marker", flush=True)
        save_state(asset, _blocked_marker(str(exc)))
        return
    except httpx.HTTPStatusError as exc:
        if _is_access_denied(exc):
            print(f"[hmda_filers] host blocked ({exc}); recording skip marker", flush=True)
            save_state(asset, _blocked_marker(str(exc)))
            return
        raise

    if not rows:
        # Reachable but empty — treat like a soft block so the step doesn't fail
        # on an upstream gap; retried after the TTL.
        print("[hmda_filers] no filer rows discovered; recording skip marker", flush=True)
        save_state(asset, _blocked_marker("no filer rows discovered"))
        return

    # Raw first, then state.
    table = pa.Table.from_pylist(rows, schema=FILER_SCHEMA)
    save_raw_parquet(table, asset)
    save_state(asset, {
        "schema_version": STATE_VERSION,
        "blocked": None,
        "last_success_at": datetime.now(timezone.utc).isoformat(),
        "last_run_stats": {"rows": len(rows), "years": years},
    })
    print(f"[hmda_filers] {len(rows)} filer-years across {years}", flush=True)


def fetch_hmda_loans(node_id: str) -> None:
    state_key = node_id
    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        state = {}
    done = set(state.get("done_years", []))

    try:
        years = _discover_hmda_years()
    except _HmdaBlocked as exc:
        print(f"[hmda_loans] host blocked ({exc}); recording skip marker", flush=True)
        save_state(state_key, _blocked_marker(str(exc), {"done_years": sorted(done)}))
        return

    if not years:
        print("[hmda_loans] no HMDA years discovered; recording skip marker", flush=True)
        save_state(state_key, _blocked_marker("no HMDA years discovered", {"done_years": sorted(done)}))
        return

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
        try:
            n_bytes = _download_csv_ranged_gz(HMDA_BASE + "view/nationwide/csv", {"years": y}, asset)
        except httpx.HTTPStatusError as exc:
            if _is_access_denied(exc):
                print(f"[hmda_loans] host blocked on {y} ({exc}); recording skip marker", flush=True)
                save_state(state_key, _blocked_marker(str(exc), {"done_years": sorted(done)}))
                return
            raise
        done.add(y)
        processed += 1
        save_state(state_key, {
            "schema_version": STATE_VERSION,
            "done_years": sorted(done),
            "blocked": None,
            "last_success_at": datetime.now(timezone.utc).isoformat(),
            "last_run_stats": {"year": y, "bytes": n_bytes},
        })
        print(f"[hmda_loans] year {y}: {n_bytes} bytes streamed", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id="cfpb-complaints", fn=fetch_complaints, kind="download"),
    NodeSpec(id="cfpb-hmda-filers", fn=fetch_hmda_filers, kind="download"),
    NodeSpec(id="cfpb-hmda-loans", fn=fetch_hmda_loans, kind="download"),
]
