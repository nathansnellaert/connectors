"""UNdata connector — SDMX 2.1 REST API at data.un.org.

Mechanism (research-chosen): `sdmx_21`. The machine-readable surface exposes
exactly 15 dataflows. Each `/data/{DataFlowID}` call returns the ENTIRE dataflow
(every country x indicator x time series) in one request — that IS the bulk path;
there is no all-corpus archive. We request SDMX-CSV (`Accept: application/
vnd.sdmx.data+csv`), which yields a flat table: DATAFLOW, <dimensions...>,
TIME_PERIOD, OBS_VALUE, <attributes...>.

Fetch shape: STATELESS FULL RE-PULL (shape 1). The API exposes no since/cursor/
modifiedAfter filter — only SDMX startPeriod/endPeriod — so each refresh re-pulls
the whole dataflow and overwrites. Corpus is ~15 modest time-series dataflows,
cheap to re-pull in full; revisions are picked up for free.

Normalization (done in Python so the SQL transform stays a thin typed pass):
per the SDMX-CSV column convention, every column BEFORE OBS_VALUE (other than
DATAFLOW and TIME_PERIOD) is a dimension; everything AFTER OBS_VALUE is an
attribute (LAST_UPDATE, OBS_STATUS, ...). We fold the dimension columns into a
single stable `series_key` ("DIM=value|DIM=value|...") so all 15 differently-
shaped dataflows land in ONE uniform raw schema, letting a single generic SQL
transform publish each. Attribute columns are dropped.
"""
import csv
import io

import httpx
import pyarrow as pa
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, raw_parquet_writer

# The entity union — the 15 SDMX dataflow ids exposed by the machine-readable API.
ENTITY_IDS = [
    "DF_SDG_GLH",
    "DF_SEEA_AEA",
    "DF_SEEA_ENERGY",
    "DF_UNDATA_COUNTRYDATA",
    "DF_UNDATA_ENERGY",
    "DF_UNDATA_MDG",
    "DF_UNDATA_WDI",
    "DF_UNData_EnergyBalance",
    "DF_UNData_UIS",
    "DF_UNData_UNFCC",
    "NASEC_IDCFINA_A",
    "NASEC_IDCFINA_Q",
    "NASEC_IDCNFSA_A",
    "NASEC_IDCNFSA_Q",
    "NA_MAIN",
]

BASE = "https://data.un.org/ws/rest"
CSV_ACCEPT = "application/vnd.sdmx.data+csv"

# Uniform raw schema across all dataflows. series_key flattens the SDMX
# dimensions; time_period is kept as the raw SDMX string ("2004", "2020-Q1");
# obs_value parsed to double (null when blank/non-numeric).
SCHEMA = pa.schema([
    ("dataflow", pa.string()),
    ("series_key", pa.string()),
    ("time_period", pa.string()),
    ("obs_value", pa.float64()),
])

BATCH_ROWS = 100_000

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
def _fetch_csv(flow_id: str) -> str:
    """Pull the entire dataflow as SDMX-CSV. One request = whole dataflow."""
    resp = get(
        f"{BASE}/data/{flow_id}",
        headers={"Accept": CSV_ACCEPT},
        timeout=(10.0, 300.0),
    )
    resp.raise_for_status()
    return resp.text


def _float_or_none(v: str):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    flow_id = node_id[len("undata-"):]
    # Recover the original-cased dataflow id (node ids are lowercased).
    flow_id = next(e for e in ENTITY_IDS if e.lower().replace("_", "-") == flow_id)

    text = _fetch_csv(flow_id)
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if not header or "OBS_VALUE" not in header:
        raise AssertionError(f"{flow_id}: unexpected SDMX-CSV header: {header}")

    obs_idx = header.index("OBS_VALUE")
    tp_idx = header.index("TIME_PERIOD")
    # Dimensions = columns before OBS_VALUE, excluding DATAFLOW (col 0) and TIME_PERIOD.
    dim_idxs = [
        i for i in range(1, obs_idx)
        if i != tp_idx and header[i] != "DATAFLOW"
    ]
    dim_names = [header[i] for i in dim_idxs]

    n_rows = 0
    with raw_parquet_writer(asset, SCHEMA) as writer:
        batch: list[dict] = []
        for row in reader:
            if len(row) <= obs_idx:
                continue
            series_key = "|".join(
                f"{name}={row[i]}" for name, i in zip(dim_names, dim_idxs) if row[i] != ""
            )
            batch.append({
                "dataflow": flow_id,
                "series_key": series_key,
                "time_period": row[tp_idx],
                "obs_value": _float_or_none(row[obs_idx]),
            })
            if len(batch) >= BATCH_ROWS:
                writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
                n_rows += len(batch)
                batch = []
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
            n_rows += len(batch)

    if n_rows == 0:
        raise AssertionError(f"{flow_id}: parsed 0 observation rows from SDMX-CSV")
    print(f"{flow_id}: wrote {n_rows} observations", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"undata-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]

# One published Delta table per dataflow. Thin typed pass over the uniform raw:
# keep the flattened series identity, the raw SDMX period, a best-effort year,
# and the numeric value (drop null observations). DISTINCT guards against any
# duplicate observation rows the source may emit.
TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f'''
            SELECT DISTINCT
                dataflow,
                series_key,
                time_period,
                TRY_CAST(time_period AS INTEGER) AS year,
                CAST(obs_value AS DOUBLE)         AS value
            FROM "{s.id}"
            WHERE obs_value IS NOT NULL
        ''',
    )
    for s in DOWNLOAD_SPECS
]
