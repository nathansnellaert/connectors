"""BIS bulk-CSV connector.

One download node per BIS dataflow (the entity union). For each dataflow we
fetch the persistent bulk export
``https://data.bis.org/static/bulk/{ID}_csv_flat.zip`` (the *flat* / tidy-long
variant — the ``_csv_col`` variant pivots every time period into its own column,
producing 30k+ columns, so it is unusable). Each zip holds a single CSV with
SDMX-style headers ``CODE:Label`` (e.g. ``FREQ:Frequency``,
``TIME_PERIOD:Time period or range``, ``OBS_VALUE:Observation Value``). The
dimension set differs per dataflow (each has its own DSD).

Raw format: **parquet with an explicit all-VARCHAR schema** built from the CSV
header at fetch time. Every cell is stored as text. This is deliberate — the
prior NDJSON attempt let DuckDB's ``read_json_auto`` infer column types from a
sample, and it guessed ``DATE`` for ``TIME_PERIOD`` (which is "1948" for annual
dataflows, "2024-Q1" for quarterly, "2002-06-03" for daily) and ``JSON`` for
columns that are all-null in the sample (``TIME_FORMAT``, ``OBS_PRE_BREAK``),
then errored at the first later line that didn't match. An explicit VARCHAR
parquet schema removes inference entirely, so DuckDB reads every column as text
and the transform does the casting with ``TRY_CAST``.

Strategy: stateless full re-pull. Each zip is the entire history of its topic
and the URLs are persistent across releases, so we re-fetch and overwrite every
run; the maintain step gates whether a given fetch runs. The largest dataflow
(WS_LBS_D_PUB) is a ~356MB zip / multi-GB CSV, so parsing is streamed straight
from the zip member into a row-group-streamed parquet writer — the full table is
never materialised in memory.
"""

import csv
import io
import re
import time
import zipfile

import httpx
import pyarrow as pa
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import (
    NodeSpec,
    SqlNodeSpec,
    get,
    load_state,
    raw_parquet_writer,
    save_state,
)

STATE_VERSION = 1
BASE_URL = "https://data.bis.org/static/bulk"
SKIP_TTL_SECONDS = 14 * 86400
BATCH_ROWS = 50_000  # rows per parquet row group flush

# The entity union — BIS dataflow ids that rank scored at/above the publish
# threshold. One download + one transform per id.
ENTITY_IDS = [
    "WS_CBPOL",
    "WS_CBS_PUB",
    "WS_CBTA",
    "WS_CPMI_CASHLESS",
    "WS_CPMI_CT1",
    "WS_CPMI_CT2",
    "WS_CPMI_DEVICES",
    "WS_CPMI_INSTITUT",
    "WS_CPMI_MACRO",
    "WS_CPMI_PARTICIP",
    "WS_CPMI_SYSTEMS",
    "WS_CPP",
    "WS_CREDIT_GAP",
    "WS_DEBT_SEC2_PUB",
    "WS_DER_OTC_TOV",
    "WS_DPP",
    "WS_DSR",
    "WS_EER",
    "WS_GLI",
    "WS_LBS_D_PUB",
    "WS_LONG_CPI",
    "WS_NA_SEC_DSS",
    "WS_OTC_DERIV2",
    "WS_SPP",
    "WS_TC",
    "WS_XRU",
    "WS_XTD_DERIV",
]


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
def _fetch_zip(url: str) -> bytes:
    # read timeout is per-chunk, generous enough for the ~356MB LBS export.
    resp = get(url, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp.content


def _entity_id(node_id: str) -> str:
    return node_id[len("bis-"):].upper().replace("-", "_")


_COL_CODE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*:")


def _column_codes(header_fields: list[str]) -> list[str]:
    """Reconstruct SDMX-CSV column codes from a bulk-flat header row.

    BIS bulk headers are ``CODE:Label`` per column, except the first three
    bare columns ``STRUCTURE``, ``STRUCTURE_ID``, ``ACTION``. The labels are
    NOT quoted, and some contain commas — e.g. WS_NA_SEC_DSS carries
    ``STO:Stocks, Transactions, Other Flows`` and
    ``EXPENDITURE:Expenditure (COFOG, COICOP, COPP or COPNI)``. A CSV split of
    such a header over-segments it into more fields than the (properly quoted)
    data rows have, so a naive ``code = field.split(":")[0]`` inflates the
    column count and every data row is then dropped on the width check.

    Real columns are: the first three bare ones, then every fragment that opens
    a new ``CODE:`` token. Comma-split label fragments (" Transactions",
    " COICOP", ...) lack that pattern and are merged away. The caller asserts
    the reconstructed count equals the data-row field count, so a genuine
    format change still surfaces loudly instead of silently dropping rows.
    """
    codes: list[str] = []
    for i, field in enumerate(header_fields):
        if i < 3:
            codes.append(field.strip())
        elif _COL_CODE_RE.match(field):
            codes.append(field.split(":", 1)[0].strip())
    return codes


def _mark_skipped(asset: str, eid: str, reason: str) -> None:
    state = load_state(asset)
    skipped = state.get("skipped", {})
    skipped[eid] = {"reason": reason, "expires_at": int(time.time()) + SKIP_TTL_SECONDS}
    state["skipped"] = skipped
    state["schema_version"] = STATE_VERSION
    save_state(asset, state)


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    eid = _entity_id(node_id)
    url = f"{BASE_URL}/{eid}_csv_flat.zip"

    try:
        content = _fetch_zip(url)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code != 429 and 400 <= code < 500:
            # Permanent: this dataflow's bulk export is gone. Mark skipped with a
            # TTL and return so one bad entity doesn't sink the whole step.
            print(f"{asset}: permanent HTTP {code} for {url}; skipping", flush=True)
            _mark_skipped(asset, eid, f"HTTP {code}")
            return
        raise

    zf = zipfile.ZipFile(io.BytesIO(content))
    members = zf.namelist()
    if not members:
        raise AssertionError(f"{asset}: empty zip from {url}")
    member = members[0]

    with zf.open(member) as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8-sig", newline="")
        reader = csv.reader(text)
        header = next(reader)
        # SDMX bulk headers are "CODE:Label"; some labels carry unquoted commas,
        # so reconstruct the real column codes rather than splitting blindly.
        keys = _column_codes(header)
        if len(set(keys)) != len(keys):
            raise AssertionError(f"{asset}: duplicate column codes in header {keys}")
        try:
            obs_idx = keys.index("OBS_VALUE")
        except ValueError:
            raise AssertionError(f"{asset}: OBS_VALUE column missing, header={keys}")
        ncols = len(keys)

        # Every column stored as VARCHAR — no type inference anywhere downstream.
        schema = pa.schema([(k, pa.string()) for k in keys])

        n = 0
        skipped_width = 0
        cols: list[list] = [[] for _ in range(ncols)]
        with raw_parquet_writer(asset, schema, compression="zstd") as writer:
            def _flush() -> None:
                if not cols[0]:
                    return
                batch = pa.record_batch(
                    [pa.array(c, type=pa.string()) for c in cols], schema=schema
                )
                writer.write_batch(batch)
                for c in cols:
                    c.clear()

            for row in reader:
                if len(row) != ncols:
                    skipped_width += 1  # width drift vs reconstructed header
                    continue
                if not row[obs_idx]:
                    continue  # series-definition row with no observation
                for i in range(ncols):
                    v = row[i]
                    cols[i].append(v if v != "" else None)
                n += 1
                if len(cols[0]) >= BATCH_ROWS:
                    _flush()
                    if n % 500_000 == 0:
                        print(f"{asset}: {n} observations", flush=True)
            _flush()

    if n == 0:
        raise AssertionError(
            f"{asset}: zip {url} yielded 0 observations — format may have changed"
        )
    # A few stray malformed lines are tolerable; a width mismatch that drops more
    # rows than it keeps means the reconstructed header no longer aligns to the
    # data — surface it rather than publish a silently-truncated table.
    if skipped_width > n:
        raise AssertionError(
            f"{asset}: {skipped_width} rows dropped on width mismatch vs "
            f"{n} kept ({ncols} cols expected) — header/data layout drifted"
        )

    print(f"{asset}: wrote {n} observations from {url}", flush=True)

    # Raw written first; record run stats afterwards.
    state = load_state(asset)
    state["schema_version"] = STATE_VERSION
    state["last_run_stats"] = {"records": n, "url": url}
    save_state(asset, state)


DOWNLOAD_SPECS: list[NodeSpec] = [
    NodeSpec(
        id=f"bis-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]


# One published Delta table per dataflow. Every raw column is VARCHAR, so the
# SQL is generic: drop the constant SDMX envelope columns (STRUCTURE, ACTION),
# cast OBS_VALUE to DOUBLE, and keep every observation that carries a numeric
# value. TIME_PERIOD stays text because its granularity ranges from annual
# ("1948") through quarterly ("2024-Q1") to daily ("2002-06-03") across
# dataflows; downstream consumers parse it per FREQ.
TRANSFORM_SPECS: list[SqlNodeSpec] = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f'''
            SELECT * EXCLUDE (STRUCTURE, ACTION)
                   REPLACE (TRY_CAST(OBS_VALUE AS DOUBLE) AS OBS_VALUE)
            FROM "{s.id}"
            WHERE TRY_CAST(OBS_VALUE AS DOUBLE) IS NOT NULL
        ''',
    )
    for s in DOWNLOAD_SPECS
]
