"""Bundesbank SDMX 2.1 connector — one Delta table per dataflow.

Access mechanism: Bundesbank statistics REST API (SDMX 2.1) at
https://api.statistiken.bundesbank.de/rest. We pull the *whole corpus* of
each dataflow in one request via GET /rest/data/{flowRef}.

Format choice (verified by probing):
  - `?format=csv` returns Bundesbank-CSV but caps at 200 time series and 406s
    on any larger dataflow (most of them).
  - `?format=sdmx_csv` returns clean long-format SDMX-CSV but repeats giant
    German/English comment columns on every observation row — BBEX3 alone is
    2.3 GB over the wire. Unusable.
  - Accept: application/vnd.bbk.data+csv-zip returns a ZIP of compact
    Bundesbank-CSV member files (split at <=200 series each), ~8 MB for BBEX3.
    This is the only path that is both complete and compact, so we use it.

Each member CSV is *wide*: row 0 is a header (`""`, then alternating
`<series_key>` / `<series_key>_FLAGS` columns), followed by a block of metadata
rows (first cell is a German label) and then observation rows (first cell is a
period like `1991`, `1967-06`, `2020-03-15`). We melt every member to one
uniform long schema shared across all 82 dataflows:
    dataflow_id, series_key, time_period, value, flag

Refresh shape: stateless full re-pull + overwrite. There is no incremental
query surface beyond startPeriod/endPeriod date filters, and research confirms
full-corpus-per-refresh is the expected pattern; re-pulling picks up revisions
for free. Large flows (e.g. BBDE1) can exceed 10 MB and stream slowly, so we
use generous read timeouts and a streaming parquet writer to bound memory.
"""

import csv
import io
import re
import zipfile

import httpx
import pyarrow as pa
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, raw_parquet_writer

ENDPOINT = "https://api.statistiken.bundesbank.de/rest"
CSV_ZIP_ACCEPT = "application/vnd.bbk.data+csv-zip;version=1.0.0"

# The entity union — the authoritative coverage target (82 dataflows).
ENTITY_IDS = [
    "BBAF3", "BBAI3", "BBAPV", "BBASV", "BBBEK1", "BBBEK2", "BBBEK3", "BBBEK4",
    "BBBEK5", "BBBK1", "BBBK10", "BBBK11", "BBBK12", "BBBK2", "BBBK3", "BBBK4",
    "BBBK5", "BBBK6", "BBBK7", "BBBK8", "BBBK9", "BBBP1", "BBBPS", "BBBS2",
    "BBBU2", "BBBZ1", "BBDA1", "BBDB2", "BBDE1", "BBDL1", "BBDP1", "BBDR1",
    "BBDY1", "BBDZ1", "BBEE1", "BBEE5", "BBEX3", "BBFBOPV", "BBFEPOEV",
    "BBFFDIPV", "BBFFDITV", "BBFI1", "BBGFS1", "BBIB1", "BBIG1", "BBIM1",
    "BBIN1", "BBK10", "BBMF1", "BBMFK1", "BBMMB", "BBMME", "BBMMS", "BBMMU",
    "BBNZ1", "BBSAP", "BBSDI", "BBSDP", "BBSEI", "BBSF2", "BBSF3", "BBSHI",
    "BBSIS", "BBSSY", "BBUMF", "BBWCAF1", "BBXE1", "BBXF1", "BBXL3", "BBXN1",
    "BBXP2", "BBXS1", "BBZVS01", "BBZVS02", "BBZVS03", "BBZVS04", "BBZVS05",
    "BBZVS06", "BBZVS08", "BBZVS11", "BBZVS12", "BBZVSSSI",
]

# Long-format observation schema, shared across every dataflow. Declared once;
# every melted batch conforms to it.
SCHEMA = pa.schema([
    ("dataflow_id", pa.string()),
    ("series_key", pa.string()),
    ("time_period", pa.string()),
    ("value", pa.float64()),
    ("flag", pa.string()),
])

# Observation rows start with a 4-digit year, optionally followed by a
# sub-year period (-MM, -Q1, -S1, -W03, -MM-DD). Metadata rows always start
# with a German label or empty string, so this cleanly separates the two.
_PERIOD_RE = re.compile(r"^\d{4}([-/].+)?$")

# Flush a batch every this many observation rows to bound memory on large
# dataflows (each member CSV holds <=200 series, so a batch is <=200k rows).
_FLUSH_ROWS = 1000

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
def _fetch_zip(dataflow_id: str) -> bytes:
    url = f"{ENDPOINT}/data/{dataflow_id}"
    resp = get(
        url,
        headers={"Accept": CSV_ZIP_ACCEPT},
        timeout=(10.0, 600.0),  # large flows (BBDE1) stream slowly
    )
    resp.raise_for_status()
    return resp.content


def _parse_value(raw: str) -> float | None:
    """Bundesbank-CSV uses '.' for missing and dot-decimal for numbers."""
    v = raw.strip()
    if v in (".", "", "-"):
        return None
    try:
        return float(v)
    except ValueError:
        # Defensive: tolerate a stray comma-decimal without crashing the flow.
        try:
            return float(v.replace(".", "").replace(",", "."))
        except ValueError:
            return None


def _series_columns(header: list[str]) -> list[tuple[str, int, int | None]]:
    """Map each value column to (series_key, value_idx, flag_idx)."""
    cols: list[tuple[str, int, int | None]] = []
    for idx in range(1, len(header)):
        name = header[idx]
        if name.endswith("_FLAGS"):
            continue
        flag_idx = None
        if idx + 1 < len(header) and header[idx + 1] == f"{name}_FLAGS":
            flag_idx = idx + 1
        cols.append((name, idx, flag_idx))
    return cols


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    dataflow_id = node_id[len("bundesbank-"):].replace("-", "_").upper()

    content = _fetch_zip(dataflow_id)
    if content[:2] != b"PK":
        raise AssertionError(
            f"{dataflow_id}: expected a ZIP payload, got {content[:80]!r}"
        )

    zf = zipfile.ZipFile(io.BytesIO(content))
    members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not members:
        raise AssertionError(f"{dataflow_id}: ZIP contains no CSV members")

    total = 0
    skipped_vals = 0
    with raw_parquet_writer(asset, SCHEMA) as writer:
        # buffers shared across all members for this dataflow
        b_df: list[str] = []
        b_key: list[str] = []
        b_period: list[str] = []
        b_value: list[float | None] = []
        b_flag: list[str | None] = []

        def flush() -> None:
            nonlocal total
            if not b_df:
                return
            batch = pa.table(
                {
                    "dataflow_id": b_df,
                    "series_key": b_key,
                    "time_period": b_period,
                    "value": b_value,
                    "flag": b_flag,
                },
                schema=SCHEMA,
            )
            writer.write_table(batch)
            total += len(b_df)
            b_df.clear()
            b_key.clear()
            b_period.clear()
            b_value.clear()
            b_flag.clear()

        for member in members:
            text = zf.read(member).decode("utf-8-sig")
            reader = csv.reader(io.StringIO(text), delimiter=";")
            try:
                header = next(reader)
            except StopIteration:
                continue
            cols = _series_columns(header)
            obs_rows = 0
            for row in reader:
                if not row or not _PERIOD_RE.match(row[0]):
                    continue  # metadata / title / blank row
                period = row[0]
                for series_key, vidx, fidx in cols:
                    if vidx >= len(row):
                        continue
                    val = _parse_value(row[vidx])
                    if val is None:
                        skipped_vals += 1
                        continue
                    flag = None
                    if fidx is not None and fidx < len(row):
                        flag = row[fidx].strip() or None
                    b_df.append(dataflow_id)
                    b_key.append(series_key)
                    b_period.append(period)
                    b_value.append(val)
                    b_flag.append(flag)
                obs_rows += 1
                if len(b_df) >= _FLUSH_ROWS * max(len(cols), 1):
                    flush()
            print(
                f"  {dataflow_id}/{member}: {obs_rows} obs rows, "
                f"{len(cols)} series",
                flush=True,
            )
        flush()

    print(
        f"  -> {dataflow_id}: wrote {total} observations "
        f"({skipped_vals} missing values skipped)",
        flush=True,
    )


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bundesbank-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]

# One published Delta table per dataflow. The raw is already a clean long table
# with no clean upstream natural key (series_key+time_period is unique only
# after the overlap-free full snapshot), so the runtime overwrites with this
# straight-through parse-and-type pass. The WHERE is the correctness gate.
TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f'''
            SELECT
                dataflow_id,
                series_key,
                time_period,
                CAST(value AS DOUBLE) AS value,
                flag
            FROM "{s.id}"
            WHERE value IS NOT NULL
              AND series_key IS NOT NULL
              AND time_period IS NOT NULL
        ''',
    )
    for s in DOWNLOAD_SPECS
]
