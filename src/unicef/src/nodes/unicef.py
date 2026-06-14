"""UNICEF connector — SDMX 2.1 REST, one dataflow per subset.

Mechanism: the public SDMX REST surface at
https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/ . Each entity is one
UNICEF dataflow (CME, NUTRITION, GLOBAL_DATAFLOW, ...). A single empty-key
data query (`data/UNICEF,<FLOW>,<ver>/all`) returns the entire flat table in
one request — no pagination.

Format: SDMX-CSV 1.0 requested via the `Accept` header
(`application/vnd.sdmx.data+csv;version=1.0.0`). NB: the `?format=csv` query
param is unreliable on this server — it returns an EMPTY 200 body for large
dataflows (e.g. CME) while the Accept header returns the full table. So we
pin the Accept header, never the format param.

Version: the flow ref omits the version (`data/UNICEF,<FLOW>/all`) so SDMX
resolves the latest published version automatically. Probed and verified 200
for every flow including SDG_PROG_ASSESSMENT (which is on 1.1, not 1.0) — this
avoids a 404 whenever a flow's version is bumped upstream.

SDMX-CSV 1.0 renders every coded cell as "CODE: Label" (e.g. "AFG: Afghanistan")
and the header columns as "CODE:Concept name". We split each cell on the first
": " into a `<col>` code and a `<col>_label` label (empty when the cell carries
no label, e.g. OBS_VALUE / TIME_PERIOD). Column sets differ per dataflow, so raw
is written as NDJSON (gzip), one asset per flow.

OBS_VALUE is NOT always numeric. Most flows carry a numeric measure, but a few
publish a categorical measure in OBS_VALUE — e.g. CCRI (Children's Climate and
Environment Risk Index) stores risk bands like "Extremely High" / "High" there,
with the numeric index tucked into OBS_FOOTNOTE. So the per-flow SQL transform
keeps OBS_VALUE as its original text and ADDS an `obs_value_numeric` column
(TRY_CAST to DOUBLE, NULL for categorical), dropping only rows whose OBS_VALUE
is blank. This publishes every observation rather than silently nuking whole
flows whose measure happens to be categorical (the prior numeric-only filter
produced 0 rows for CCRI and failed the DAG).

Volume: flows range from a few thousand obs (WT) to ~860k rows / 180MB (CME)
to ~870MB (GLOBAL_DATAFLOW). The response is streamed and parsed incrementally
so memory stays bounded regardless of flow size.

Refresh model: stateless full re-pull. Research could not verify the SDMX
`updatedAfter` incremental parameter, so each refresh re-fetches the whole
dataflow and overwrites — revisions and late corrections are picked up for free.
A modest per-flow cost (one request each, ~62 flows source-wide, 35 published),
acceptable for a statistical source that publishes infrequently.
"""
from __future__ import annotations

import csv
import io
import json

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get_client, raw_writer

BASE = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/"
# SDMX-CSV 1.0 with labels — flat code+label table, no XML/JSON tree-walking.
CSV_ACCEPT = "application/vnd.sdmx.data+csv;version=1.0.0"

# Entity union (authoritative coverage target) — copied from
# data/sources/unicef/steps/534fb1dfd81640fc96121a7e73b1c9a7/entity_union.json
ENTITY_IDS = [
    "CAUSE_OF_DEATH", "CCRI", "CHILD_RELATED_SDG", "CHLD_PVTY", "CME",
    "CME_CAUSE_OF_DEATH", "CME_SUBNATIONAL", "DM", "DM_PROJECTIONS", "ECD",
    "ECONOMIC", "EDUCATION", "EDUCATION_FLS", "EDUCATION_LG",
    "EDUCATION_UIS_SDG", "FUNCTIONAL_DIFF", "GENDER", "GLOBAL_DATAFLOW",
    "HIV_AIDS", "IMMUNISATION", "MG", "MG_FLOW", "MNCH", "NUTRITION", "PT",
    "PT_CM", "PT_CONFLICT", "PT_FGM", "SDG_PROG_ASSESSMENT", "SOC_PROTECTION",
    "WASH_HEALTHCARE_FACILITY", "WASH_HOUSEHOLDS", "WASH_HOUSEHOLD_MH",
    "WASH_SCHOOLS", "WT",
]

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


class _ByteStreamReader(io.RawIOBase):
    """Adapt an httpx byte-chunk iterator into a binary readable stream so a
    TextIOWrapper + csv.reader can parse it incrementally (handles quoted
    fields with embedded newlines/commas) without buffering the whole body."""

    def __init__(self, byte_iter):
        self._it = byte_iter
        self._buf = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        while not self._buf:
            try:
                self._buf = next(self._it)
            except StopIteration:
                return 0
        n = min(len(b), len(self._buf))
        b[: n] = self._buf[: n]
        self._buf = self._buf[n:]
        return n


def _split_cell(value: str) -> tuple[str, str]:
    """SDMX-CSV 1.0 coded cell 'CODE: Label' -> (code, label). Cells with no
    label (OBS_VALUE, TIME_PERIOD, free-text) -> (value, '')."""
    parts = value.split(": ", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return value, ""


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _download_flow(asset: str, flow: str) -> int:
    """Stream one dataflow's SDMX-CSV and write it as NDJSON.gz. Returns the
    number of observation rows written. Retried as a whole on transient errors;
    raw_writer truncates on each attempt, so a retry is idempotent."""
    # Version omitted -> SDMX resolves the latest published version.
    url = f"{BASE}data/UNICEF,{flow}/all"
    client = get_client()
    written = 0
    with client.stream(
        "GET", url, headers={"Accept": CSV_ACCEPT}, timeout=(10.0, 600.0)
    ) as resp:
        resp.raise_for_status()  # inside the retry: 5xx/429 -> retried, 4xx -> raises
        byte_iter = resp.iter_bytes(chunk_size=1 << 20)
        text = io.TextIOWrapper(
            io.BufferedReader(_ByteStreamReader(byte_iter)),
            encoding="utf-8",
            newline="",
        )
        reader = csv.reader(text)
        try:
            header = next(reader)
        except StopIteration:
            header = []
        # "CODE:Concept name" -> clean lowercase code column name.
        cols = [h.split(":", 1)[0].strip().lower() for h in header]

        with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as out:
            for row in reader:
                rec = {}
                for col, cell in zip(cols, row):
                    if col == "dataflow":
                        continue  # constant per flow — redundant
                    code, label = _split_cell(cell)
                    rec[col] = code
                    rec[col + "_label"] = label
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                written += 1
                if written % 100000 == 0:
                    print(f"  {asset}: {written} rows", flush=True)
    return written


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity = node_id[len("unicef-"):].replace("-", "_").upper()
    n = _download_flow(asset, entity)
    print(f"{asset}: wrote {n} rows (flow={entity})", flush=True)
    if n == 0:
        # A flow that exists but returns no observations is a real anomaly —
        # surface it rather than publishing an empty table.
        raise ValueError(f"{asset}: dataflow {entity} returned 0 observations")


DOWNLOAD_SPECS: list[NodeSpec] = [
    NodeSpec(
        id=f"unicef-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]

# One published Delta table per dataflow. The SQL is uniform across flows: keep
# every column as-is, add a numeric cast of OBS_VALUE (NULL where the measure is
# categorical, e.g. CCRI), and drop only rows whose OBS_VALUE is blank. OBS_VALUE
# is mandatory in every SDMX dataflow, so a non-blank filter never empties a flow.
TRANSFORM_SPECS: list[SqlNodeSpec] = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f'''
            SELECT
                *,
                TRY_CAST(obs_value AS DOUBLE) AS obs_value_numeric
            FROM "{s.id}"
            WHERE COALESCE(TRIM(obs_value), '') <> ''
        ''',
    )
    for s in DOWNLOAD_SPECS
]
