"""Bank of Canada — Valet REST connector.

Mechanism: valet_rest (https://www.bankofcanada.ca/valet/) — no auth, no key.

Two collect entities → two download nodes:

  * ``series`` — the catalog of every individual time series (~15.6k entries),
    one shot from ``/lists/series/json``. Small, written as a single parquet.

  * ``values`` — the long-format observations for every series. The Valet
    observation endpoint accepts a comma-separated batch of series names and
    returns the FULL history of each in one shot (no pagination). We chunk the
    catalog and stream the parsed (series_id, date, value) rows into a single
    row-group-streamed parquet asset.

Fetch shape: STATELESS FULL RE-PULL. The corpus re-pulls in full every run
(~196 chunked requests for ``values``, finishing in a few minutes). Although
the API supports incremental ``start_date`` queries, a per-series watermark
across 15.6k series is far more machinery than the cost it would save, and a
full re-pull picks up revisions/late corrections for free. The maintain step
gates how often this runs. Streaming the writer keeps memory bounded despite
the full re-pull, so no batched assets / resume state are needed.
"""

import httpx
import pyarrow as pa
from ratelimit import limits, sleep_and_retry
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
    raw_parquet_writer,
    save_raw_parquet,
)

BASE = "https://www.bankofcanada.ca/valet"

# Comma-batched observation requests. 80 ids * ~49 chars stays well under any
# URL length limit, and one request returns each series' full history.
CHUNK = 80

SERIES_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("label", pa.string()),
    ("description", pa.string()),
    ("link", pa.string()),
])

# Observation values are returned by the API as strings (e.g. "1.3993"); we
# keep them as strings in raw and cast to DOUBLE in the transform (the
# correctness gate), preserving fidelity and surfacing any non-numeric drift.
VALUES_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("date", pa.string()),
    ("value", pa.string()),
])

# --------------------------------------------------------------------------- #
# HTTP — retry + polite throttling                                            #
# --------------------------------------------------------------------------- #

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


# Docs advise a gradual request rate; no published hard limit. ~5 rps is polite
# and well below anything observed. Each spec runs in its own process, so this
# per-process limiter is the only one hitting the host in that process.
@sleep_and_retry
@limits(calls=5, period=1)
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=2, max=120),
    reraise=True,
)
def _fetch_json(url: str, params: dict | None = None):
    resp = get(url, params=params or {}, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp.json()


def _list_series_ids() -> list[str]:
    data = _fetch_json(f"{BASE}/lists/series/json")
    return list(data.get("series", {}).keys())


# --------------------------------------------------------------------------- #
# series — catalog                                                            #
# --------------------------------------------------------------------------- #

def fetch_series(node_id: str) -> None:
    """Fetch the full series catalog and write it as one parquet asset."""
    asset = node_id
    data = _fetch_json(f"{BASE}/lists/series/json")
    series = data.get("series", {})
    rows = [
        {
            "series_id": sid,
            "label": meta.get("label"),
            "description": meta.get("description"),
            "link": meta.get("link"),
        }
        for sid, meta in series.items()
    ]
    table = pa.Table.from_pylist(rows, schema=SERIES_SCHEMA)
    save_raw_parquet(table, asset)
    print(f"series: wrote {len(rows)} catalog rows", flush=True)


# --------------------------------------------------------------------------- #
# values — observations                                                       #
# --------------------------------------------------------------------------- #

def _parse_observations(data: dict) -> list[dict]:
    """Flatten the {d, <series_id>: {v}} row shape to long format."""
    rows: list[dict] = []
    for obs in data.get("observations", []):
        d = obs.get("d")
        for key, val in obs.items():
            if key == "d":
                continue
            if isinstance(val, dict):
                v = val.get("v")
                if v is not None and v != "":
                    rows.append({"series_id": key, "date": d, "value": str(v)})
    return rows


def _fetch_chunk(chunk: list[str]) -> list[dict]:
    """Observations for a batch of series. On a 404 (a delisted/invalid id in
    the batch) bisect to single-series requests and skip the offending one."""
    names = ",".join(chunk)
    url = f"{BASE}/observations/{names}/json"
    try:
        data = _fetch_json(url)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            if len(chunk) == 1:
                print(f"values: skip {chunk[0]} (404 not found)", flush=True)
                return []
            rows: list[dict] = []
            for sid in chunk:
                rows.extend(_fetch_chunk([sid]))
            return rows
        raise
    return _parse_observations(data)


def fetch_values(node_id: str) -> None:
    """Stream all observations for the full series catalog into one parquet.

    Stateless: the writer is rebuilt from scratch every run, overwriting the
    single ``values`` asset. Memory stays bounded — one chunk's rows are held,
    written as a row group, then released."""
    asset = node_id
    series_ids = _list_series_ids()
    total_chunks = (len(series_ids) + CHUNK - 1) // CHUNK
    total_rows = 0
    with raw_parquet_writer(asset, VALUES_SCHEMA) as writer:
        for ci in range(total_chunks):
            chunk = series_ids[ci * CHUNK:(ci + 1) * CHUNK]
            rows = _fetch_chunk(chunk)
            if rows:
                writer.write_table(pa.Table.from_pylist(rows, schema=VALUES_SCHEMA))
                total_rows += len(rows)
            if ci % 20 == 0:
                done = min((ci + 1) * CHUNK, len(series_ids))
                print(
                    f"values: {done}/{len(series_ids)} series, {total_rows} rows",
                    flush=True,
                )
    print(f"values: done, {total_rows} observation rows", flush=True)


# --------------------------------------------------------------------------- #
# Specs                                                                        #
# --------------------------------------------------------------------------- #

DOWNLOAD_SPECS = [
    NodeSpec(id="bank-of-canada-series", fn=fetch_series, kind="download"),
    NodeSpec(id="bank-of-canada-values", fn=fetch_values, kind="download"),
]

TRANSFORM_SPECS = [
    SqlNodeSpec(
        id="bank-of-canada-series-transform",
        deps=["bank-of-canada-series"],
        sql='''
            SELECT
                series_id,
                label,
                description,
                link
            FROM "bank-of-canada-series"
            WHERE series_id IS NOT NULL
        ''',
    ),
    SqlNodeSpec(
        id="bank-of-canada-values-transform",
        deps=["bank-of-canada-values"],
        sql='''
            SELECT
                series_id,
                CAST(date AS DATE)      AS date,
                TRY_CAST(value AS DOUBLE) AS value
            FROM "bank-of-canada-values"
            WHERE series_id IS NOT NULL
              AND date IS NOT NULL
              AND TRY_CAST(value AS DOUBLE) IS NOT NULL
        ''',
    ),
]
