"""Bank of Japan time-series statistics — REST API v1 (stat-search.boj.or.jp).

Catalog connector: one download spec per BOJ database (entity). Each spec:
  1. GET /getMetadata?db=<DB> to enumerate every series code + its frequency.
  2. Group codes by frequency (the API requires ONE frequency per /getDataCode
     call) and batch each group into chunks of <=250 codes.
  3. For each chunk, GET /getDataCode and follow the NEXTPOSITION cursor
     (returned when the ~60,000-data-point response cap splits the chunk across
     pages) until it comes back null.
  4. Explode each series' parallel SURVEY_DATES / VALUES arrays into flat
     (series_code, period, value) observation rows, streamed to one parquet
     asset per DB.

Stateless full re-pull every refresh: the API exposes no incremental / since
filter (the documented contract is a full corpus re-pull), and revisions/late
corrections are picked up for free. Some DBs are large (CO ~166k series, FF/BIS
~34k) so raw is written through a streaming parquet writer to bound memory.

Period encoding varies by frequency (parsed to a DATE in the transform):
  DAILY/WEEKLY -> YYYYMMDD, MONTHLY -> YYYYMM, QUARTERLY -> YYYYQ (Q in 01-04),
  SEMIANNUAL -> YYYYH (H in 01-02), ANNUAL -> YYYY.
"""

import httpx
import pyarrow as pa
from ratelimit import limits, sleep_and_retry
from tenacity import (
    retry, retry_if_exception, stop_after_attempt, wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, raw_parquet_writer

BASE_URL = "https://www.stat-search.boj.or.jp/api/v1/"

# Documented coverage: the 46 databases the rank step kept (entity union).
ENTITY_IDS = [
    "BIS", "BP01", "BS01", "BS02", "CO", "DER", "FF",
    "FM01", "FM02", "FM03", "FM04", "FM05", "FM06", "FM08", "FM09",
    "IR01", "IR02", "IR03", "IR04",
    "LA01", "LA02", "LA03", "LA04", "LA05",
    "MD01", "MD02", "MD03", "MD05", "MD06", "MD07", "MD08", "MD09",
    "MD10", "MD11", "MD12", "MD13", "MD14",
    "OB01", "OB02", "PF01", "PF02", "PR01", "PR02", "PR04", "PS01", "PS02",
]

# API caps: <=250 codes and one frequency per /getDataCode request.
CODES_PER_REQUEST = 250
# Safety ceiling on cursor pages for a single chunk — far above any real DB
# (the data-point cap splits a 250-code chunk into a handful of pages). Firing
# this means the cursor is looping and we must surface it, not crawl forever.
MAX_PAGES_PER_CHUNK = 2000

RAW_SCHEMA = pa.schema([
    ("series_code", pa.string()),
    ("name", pa.string()),
    ("unit", pa.string()),
    ("frequency", pa.string()),
    ("category", pa.string()),
    ("last_update", pa.string()),
    ("period", pa.string()),
    ("value", pa.float64()),
])

_TRANSIENT_EXC = (
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
    httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError,
    httpx.ProxyError,
)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


# Rate limit is documented only qualitatively ("excessive access frequency may
# result in a restriction of access"). Throttle conservatively to ~1.5 req/s
# per process, well under the manual's guidance of intervals between calls.
@sleep_and_retry
@limits(calls=3, period=2)
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _api_get(endpoint: str, params: dict) -> dict:
    resp = get(
        BASE_URL + endpoint,
        params=params,
        headers={"Accept-Encoding": "gzip"},
        timeout=(10.0, 180.0),
    )
    resp.raise_for_status()
    return resp.json()


def _fetch_metadata(db: str) -> list[dict]:
    """Return series metadata rows for a DB (rows with a real SERIES_CODE)."""
    payload = _api_get("getMetadata", {"format": "json", "lang": "en", "db": db})
    rows = payload.get("RESULTSET") or []
    return [r for r in rows if r.get("SERIES_CODE")]


def _explode(result_rows: list[dict]) -> list[dict]:
    """Flatten /getDataCode rows into observation dicts."""
    out = []
    for r in result_rows:
        vals = r.get("VALUES") or {}
        dates = vals.get("SURVEY_DATES") or []
        values = vals.get("VALUES") or []
        lu = r.get("LAST_UPDATE")
        base = {
            "series_code": r.get("SERIES_CODE"),
            "name": r.get("NAME_OF_TIME_SERIES"),
            "unit": r.get("UNIT"),
            "frequency": r.get("FREQUENCY"),
            "category": r.get("CATEGORY"),
            "last_update": str(lu) if lu is not None else None,
        }
        for d, v in zip(dates, values):
            row = dict(base)
            row["period"] = str(d) if d is not None else None
            row["value"] = float(v) if v is not None else None
            out.append(row)
    return out


def _fetch_chunk(db: str, freq: str, codes: list[str]) -> list[dict]:
    """Fetch all pages for one (frequency, <=250 codes) chunk; explode rows."""
    out = []
    start_position = None
    pages = 0
    while True:
        params = {
            "format": "json", "lang": "en", "db": db,
            "code": ",".join(codes),
        }
        if start_position is not None:
            params["startPosition"] = start_position
        payload = _api_get("getDataCode", params)
        out.extend(_explode(payload.get("RESULTSET") or []))
        pages += 1
        nxt = payload.get("NEXTPOSITION")
        if not nxt:
            break
        if pages >= MAX_PAGES_PER_CHUNK:
            raise RuntimeError(
                f"{db}/{freq}: NEXTPOSITION cursor exceeded {MAX_PAGES_PER_CHUNK} "
                f"pages for one chunk — refusing to loop"
            )
        start_position = nxt
    return out


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    db = node_id[len("bank-of-japan-"):].upper()

    series = _fetch_metadata(db)
    if not series:
        raise RuntimeError(f"{db}: getMetadata returned no series — unexpected")

    # Group series codes by frequency (API requires one frequency per request).
    by_freq: dict[str, list[str]] = {}
    for s in series:
        by_freq.setdefault(s.get("FREQUENCY") or "", []).append(s["SERIES_CODE"])

    total_obs = 0
    total_codes = sum(len(v) for v in by_freq.values())
    done_codes = 0
    with raw_parquet_writer(asset, RAW_SCHEMA) as writer:
        for freq, codes in by_freq.items():
            if not freq:
                continue
            for i in range(0, len(codes), CODES_PER_REQUEST):
                chunk = codes[i:i + CODES_PER_REQUEST]
                rows = _fetch_chunk(db, freq, chunk)
                done_codes += len(chunk)
                if rows:
                    table = pa.Table.from_pylist(rows, schema=RAW_SCHEMA)
                    writer.write_table(table)
                    total_obs += len(rows)
                print(
                    f"{db}: {done_codes}/{total_codes} codes, "
                    f"{total_obs} observations",
                    flush=True,
                )

    print(f"{db}: done — {total_codes} series, {total_obs} observations", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bank-of-japan-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]


# One published Delta table per DB. Thin parse: cast value, parse the
# frequency-dependent period encoding to a calendar DATE, drop null values.
_DATE_SQL = """
        CASE
            WHEN frequency LIKE 'QUARTERLY%'
                THEN make_date(CAST(period[1:4] AS INTEGER),
                               (CAST(period[5:6] AS INTEGER) - 1) * 3 + 1, 1)
            WHEN frequency LIKE 'SEMIANNUAL%'
                THEN make_date(CAST(period[1:4] AS INTEGER),
                               (CAST(period[5:6] AS INTEGER) - 1) * 6 + 1, 1)
            WHEN frequency LIKE 'MONTHLY%'
                THEN make_date(CAST(period[1:4] AS INTEGER),
                               CAST(period[5:6] AS INTEGER), 1)
            WHEN frequency LIKE 'ANNUAL%' AND length(period) = 4
                THEN make_date(CAST(period AS INTEGER), 1, 1)
            WHEN length(period) = 8
                THEN CAST(strptime(period, '%Y%m%d') AS DATE)
            WHEN length(period) = 4
                THEN make_date(CAST(period AS INTEGER), 1, 1)
            ELSE NULL
        END
"""


def _transform_sql(download_id: str) -> str:
    return f'''
        SELECT
            series_code,
            name,
            unit,
            frequency,
            category,
            last_update,
            period,
            {_DATE_SQL} AS date,
            CAST(value AS DOUBLE) AS value
        FROM "{download_id}"
        -- Drop observations the source reports with a value but no survey date
        -- (a sporadic malformation seen in a few CO/TANKAN series): an
        -- observation with no period cannot be placed on a timeline.
        WHERE value IS NOT NULL AND period IS NOT NULL
    '''


TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=_transform_sql(s.id),
    )
    for s in DOWNLOAD_SPECS
]
