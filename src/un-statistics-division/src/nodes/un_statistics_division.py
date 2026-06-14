"""UN Statistics Division — UNData SDMX 2.1 hub (catalog connector).

Mechanism: sdmx_21. Each entity is one SDMX dataflow on the UNData hub
(https://data.un.org/ws/rest/). The data resource /data/{flowRef}?format=csv
returns the ENTIRE dataflow as one SDMX-CSV with no pagination — the optimal
bulk-per-entity pull. flowRef is the bare dataflow id; the agency-prefixed
form (UNSD,DF,latest) 500s on this Eurostat-SDMX-RI deployment, so we use the
bare id observed working during probing.

Fetch shape: stateless full re-pull (shape 1). The source exposes no
modified-since/ETag and only full-corpus startPeriod/endPeriod filters, so we
re-pull each dataflow in full every refresh and overwrite. Largest table is
NA_MAIN (~160MB CSV, ~1.2M rows); the rest are <13MB. A handful of small-to-
medium bulk CSVs re-pulled in a few minutes — no incremental machinery
warranted.

Raw format: parquet with EVERY column typed as string. SDMX-CSV column sets
differ per dataflow (each has its own DSD), and DuckDB's read_csv_auto
mis-types the heterogeneous columns (TIME_PERIOD is "1990" in annual rows but
"2007-Q1" in quarterly rows; OBS_VALUE carries "NA"/":" placeholders). Parsing
all-string with pyarrow is the faithful, loss-free contract; the SQL transform
does the typing (TRY_CAST OBS_VALUE -> DOUBLE) as the correctness gate. The
CSV is stream-parsed into the parquet writer so the 160MB NA_MAIN never
materializes as a full arrow table.
"""

from __future__ import annotations

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
    SqlNodeSpec,
    get,
    load_state,
    raw_parquet_writer,
    save_state,
)

STATE_VERSION = 1

BASE = "https://data.un.org/ws/rest/"

# The entity union — copied verbatim from
# data/sources/un-statistics-division/steps/dbcec4856b104bdf9cf475a5551538f0/entity_union.json
ENTITY_IDS = [
    "DF_SDG_GLH",
    "DF_SEEA_AEA",
    "DF_SEEA_ENERGY",
    "DF_UNDATA_COUNTRYDATA",
    "DF_UNDATA_ENERGY",
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


def _node_id(entity_id: str) -> str:
    return f"un-statistics-division-{entity_id.lower().replace('_', '-')}"


# node_id is lossy (lowercased, underscores->dashes), so the flowRef can't be
# recovered from it — map node_id back to the exact case-sensitive entity id.
_ENTITY_BY_NODE = {_node_id(e): e for e in ENTITY_IDS}


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
def _fetch_csv(url: str) -> bytes:
    # One bulk request returns the whole dataflow as SDMX-CSV; read timeout is
    # generous for the ~160MB NA_MAIN table.
    resp = get(
        url,
        params={"format": "csv"},
        headers={"Accept": "application/vnd.sdmx.data+csv"},
        timeout=(10.0, 300.0),
    )
    resp.raise_for_status()
    return resp.content


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity_id = _ENTITY_BY_NODE[node_id]

    content = _fetch_csv(f"{BASE}data/{entity_id}")

    # Column set is dataflow-specific; derive it from the header and force every
    # column to string so no per-row type inference can fail mid-stream.
    header_line = content.split(b"\n", 1)[0].decode("utf-8").strip()
    columns = [c.strip() for c in header_line.split(",")]
    convert_options = pacsv.ConvertOptions(
        column_types={c: pa.string() for c in columns},
        strings_can_be_null=True,
        null_values=[""],
    )
    parse_options = pacsv.ParseOptions(newlines_in_values=True)  # quoted comments span lines
    read_options = pacsv.ReadOptions(block_size=16 << 20)

    reader = pacsv.open_csv(
        pa.BufferReader(content),
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )

    total_rows = 0
    with raw_parquet_writer(asset, reader.schema) as writer:
        for batch in reader:
            if batch.num_rows:
                writer.write_batch(batch)
                total_rows += batch.num_rows

    # State is observability only (no watermark — stateless full re-pull): lets a
    # later run diff record counts to spot a silently shrunk feed.
    state = load_state(asset)
    state["schema_version"] = STATE_VERSION
    state["last_run_stats"] = {
        "records": total_rows,
        "bytes": len(content),
        "columns": len(columns),
    }
    save_state(asset, state)


DOWNLOAD_SPECS = [
    NodeSpec(id=_node_id(eid), fn=fetch_one, kind="download")
    for eid in ENTITY_IDS
]


# One published Delta table per dataflow. The SQL is uniform across all 14:
# keep every dimension/attribute column as-is, type the observation, and drop
# rows whose OBS_VALUE is absent or a non-numeric placeholder ("NA", ":").
# TIME_PERIOD stays a string — SDMX time can be "1990", "2007-Q1", "2007-01".
TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f'''
            SELECT * REPLACE (TRY_CAST(OBS_VALUE AS DOUBLE) AS OBS_VALUE)
            FROM "{s.id}"
            WHERE TRY_CAST(OBS_VALUE AS DOUBLE) IS NOT NULL
        ''',
    )
    for s in DOWNLOAD_SPECS
]
