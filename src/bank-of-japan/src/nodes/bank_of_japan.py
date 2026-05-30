"""Bank of Japan time-series statistics — download step.

Mechanism: rest_v1 (https://www.stat-search.boj.or.jp/api/v1/). Catalog
connector — one NodeSpec per BOJ database (entity). Each database holds
hundreds to tens of thousands of DB-scoped series across 11 categories.

Per database the fetch is a stateless full re-pull:
  1. GET /getMetadata?db=<DB> once to enumerate every series code, its
     frequency, and the LAYER1-5 hierarchy.
  2. Group series codes by FREQUENCY (the API rejects mixed-frequency
     /getDataCode calls) and batch each group in chunks of <=250 codes.
  3. GET /getDataCode for each chunk, following the NEXTPOSITION cursor
     (passed back as startPosition) until null — the cursor fires when the
     60,000-data-point cap is hit within a chunk. Series are returned
     complete (never split across cursor pages), confirmed by probing.

Why stateless full re-pull (not incremental): the API exposes no
since/modifiedAfter filter — the documented contract is a full corpus
re-pull per refresh. Revisions and late corrections are picked up for free.
Per-DB metadata exposes LAST_UPDATE but there is no server-side delta query,
so a watermark would only let us skip work, not fetch less per series.

Raw shape: tabular with stable column types -> streamed parquet. Each
/getDataCode page is written as one row group via raw_parquet_writer so a
large DB (e.g. BIS ~33k quarterly series) never materialises in full in RAM.
Series-level metadata (name/unit/category/layers) is denormalised onto each
observation row; zstd collapses the repetition.

Auth: none (public). Rate limit: undocumented numerically; the manual only
warns against "excessive access frequency". We throttle conservatively
(<=2 req/s) and lean on retry backoff for any 429.
"""

import time

import httpx
import pyarrow as pa
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, raw_parquet_writer

BASE_URL = "https://www.stat-search.boj.or.jp/api/v1/"
MAX_CODES_PER_CALL = 250          # API hard cap per /getDataCode request
THROTTLE_SECONDS = 0.5            # ~2 req/s — conservative; no numeric limit published
MAX_PAGES_PER_CHUNK = 2000        # safety ceiling on cursor pagination; raises if exceeded

# The entity union — every BOJ database scored at/above publish threshold.
# Copied from entity_union.json. These are the DB codes passed as ?db=<DB>.
ENTITY_IDS = [
    "BIS", "BP01", "BS01", "BS02", "CO", "DER", "FF",
    "FM01", "FM02", "FM03", "FM04", "FM05", "FM06", "FM08", "FM09",
    "IR01", "IR02", "IR03", "IR04",
    "LA01", "LA02", "LA03", "LA04", "LA05",
    "MD01", "MD02", "MD03", "MD05", "MD06", "MD07", "MD08", "MD09",
    "MD10", "MD11", "MD12", "MD13", "MD14",
    "OB01", "OB02", "PF01", "PF02",
    "PR01", "PR02", "PR04", "PS01", "PS02",
]

# Declared once; the contract for every streamed row group across all DBs.
SCHEMA = pa.schema([
    ("db", pa.string()),
    ("series_code", pa.string()),
    ("name", pa.string()),
    ("unit", pa.string()),
    ("frequency", pa.string()),
    ("category", pa.string()),
    ("layer1", pa.int64()),
    ("layer2", pa.int64()),
    ("layer3", pa.int64()),
    ("layer4", pa.int64()),
    ("layer5", pa.int64()),
    ("period", pa.int64()),
    ("value", pa.float64()),
    ("last_update", pa.int64()),
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


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _api_get(endpoint: str, params: dict) -> dict:
    """One GET against the BOJ API, retried on transient failures.

    Errors are returned as JSON regardless of the requested format; a bad
    request surfaces as an HTTP 4xx, which raise_for_status turns into a
    permanent HTTPStatusError (not retried, not transient).
    """
    resp = get(
        BASE_URL + endpoint,
        params=params,
        headers={"Accept-Encoding": "gzip"},  # API gzips when offered
        timeout=(10.0, 180.0),
    )
    resp.raise_for_status()
    return resp.json()


def _fetch_metadata(db: str) -> list[dict]:
    """Enumerate every real series in a DB (rows with a non-empty SERIES_CODE
    and FREQUENCY — category-header rows carry neither)."""
    payload = _api_get("getMetadata", {"format": "json", "lang": "en", "db": db})
    rows = payload.get("RESULTSET") or []
    return [r for r in rows if r.get("SERIES_CODE") and r.get("FREQUENCY")]


def _coerce_int(x):
    if isinstance(x, bool) or x is None:
        return None
    if isinstance(x, int):
        return x
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _coerce_float(x):
    if isinstance(x, bool) or x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(x)
    except (TypeError, ValueError):
        return None  # missing-value markers / blanks -> null


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _page_to_table(db: str, series_objs: list[dict], layers_by_code: dict) -> pa.Table:
    """Flatten a /getDataCode page (list of series, each with aligned
    SURVEY_DATES/VALUES arrays) into one Arrow table conforming to SCHEMA."""
    cols = {name: [] for name in SCHEMA.names}
    for s in series_objs:
        code = s.get("SERIES_CODE", "")
        vals = s.get("VALUES") or {}
        dates = vals.get("SURVEY_DATES") or []
        values = vals.get("VALUES") or []
        layers = layers_by_code.get(code, {})
        name = s.get("NAME_OF_TIME_SERIES", "")
        unit = s.get("UNIT", "")
        freq = s.get("FREQUENCY", "")
        category = s.get("CATEGORY", "")
        last_update = _coerce_int(s.get("LAST_UPDATE"))
        for period, value in zip(dates, values):
            p = _coerce_int(period)
            if p is None:
                # A period-less observation is unusable for a time series.
                # SURVEY_DATES very rarely carries a null/blank entry (observed
                # deep in CO's ~166k series); drop the data point rather than
                # emit a null-keyed row that no downstream transform can place.
                continue
            cols["db"].append(db)
            cols["series_code"].append(code)
            cols["name"].append(name)
            cols["unit"].append(unit)
            cols["frequency"].append(freq)
            cols["category"].append(category)
            cols["layer1"].append(layers.get("LAYER1"))
            cols["layer2"].append(layers.get("LAYER2"))
            cols["layer3"].append(layers.get("LAYER3"))
            cols["layer4"].append(layers.get("LAYER4"))
            cols["layer5"].append(layers.get("LAYER5"))
            cols["period"].append(p)
            cols["value"].append(_coerce_float(value))
            cols["last_update"].append(last_update)
    return pa.table({k: cols[k] for k in SCHEMA.names}, schema=SCHEMA)


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    db = node_id[len("bank-of-japan-"):].upper()

    meta = _fetch_metadata(db)
    if not meta:
        # No series in this DB is a genuine source signal, not a transport
        # bug — write an empty (schema-bearing) asset so reruns are stable.
        with raw_parquet_writer(asset, SCHEMA) as writer:
            writer.write_table(SCHEMA.empty_table())
        print(f"{asset}: getMetadata returned 0 series", flush=True)
        return

    layers_by_code = {
        r["SERIES_CODE"]: {f"LAYER{i}": _coerce_int(r.get(f"LAYER{i}")) for i in range(1, 6)}
        for r in meta
    }

    # The API forbids mixed frequencies in one /getDataCode call — split first.
    codes_by_freq: dict[str, list[str]] = {}
    for r in meta:
        codes_by_freq.setdefault(r["FREQUENCY"], []).append(r["SERIES_CODE"])

    total_rows = 0
    total_series = 0
    with raw_parquet_writer(asset, SCHEMA) as writer:
        for freq, codes in codes_by_freq.items():
            for chunk in _chunks(codes, MAX_CODES_PER_CALL):
                start_position = None
                pages = 0
                while True:
                    params = {
                        "format": "json", "lang": "en", "db": db,
                        "code": ",".join(chunk),
                    }
                    if start_position is not None:
                        params["startPosition"] = start_position
                    payload = _api_get("getDataCode", params)
                    series_objs = payload.get("RESULTSET") or []
                    if series_objs:
                        table = _page_to_table(db, series_objs, layers_by_code)
                        if table.num_rows:
                            writer.write_table(table)
                            total_rows += table.num_rows
                        total_series += len(series_objs)
                    pages += 1
                    if pages >= MAX_PAGES_PER_CHUNK:
                        raise RuntimeError(
                            f"{asset}: cursor pagination exceeded {MAX_PAGES_PER_CHUNK} "
                            f"pages for freq={freq} — source grew past expectations"
                        )
                    next_position = payload.get("NEXTPOSITION")
                    if not next_position:
                        break
                    start_position = next_position
                    time.sleep(THROTTLE_SECONDS)
                time.sleep(THROTTLE_SECONDS)
            print(
                f"{asset}: freq={freq} done — {len(codes)} codes, "
                f"{total_rows} rows so far",
                flush=True,
            )

    print(f"{asset}: {total_series} series fetched, {total_rows} observation rows", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bank-of-japan-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]
