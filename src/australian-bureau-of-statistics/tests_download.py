"""Health invariants for the ABS download node.

These run post-DAG inside the connector. They guard against silent
degradation that mere file-existence misses: empty payloads, a format switch
(SDMX-CSV losing its DATAFLOW/OBS_VALUE columns), accidental numeric coercion
of SDMX code columns, and mass truncation of the high-signal series.
"""

import pyarrow.parquet as pq

from subsets_utils import load_raw_parquet
from subsets_utils.config import raw_uri, get_fs

# Required SDMX-CSV columns present on every ABS dataflow.
_REQUIRED_COLS = {"DATAFLOW", "TIME_PERIOD", "OBS_VALUE"}

# Known-large dataflows with conservative row floors (real observed counts are
# far higher — these catch order-of-magnitude truncation, not normal drift).
_FLOORS = {
    "australian-bureau-of-statistics-cpi": 100_000,
    "australian-bureau-of-statistics-erp-q": 1_000_000,
    "australian-bureau-of-statistics-wpi": 10_000,
}


def _row_count(spec_id: str) -> int:
    """Row count from the parquet footer only — no full data transfer.

    Opens the asset through the same R2-aware fsspec layer the loaders use
    (`raw_uri` + `get_fs`); pyarrow reads just the footer via ranged GETs, so
    this stays cheap across all 761 assets even when some are tens of millions
    of rows. Local in dev, s3:// (R2) in cloud — never touches a raw path
    directly."""
    uri = raw_uri(spec_id, "parquet")
    with get_fs(uri).open(uri, "rb") as f:
        return pq.ParquetFile(f).metadata.num_rows


def test_every_asset_nonempty(spec_ids):
    """Every dataflow snapshot must hold at least one observation. An empty
    payload means the endpoint returned only a header (format/permission drift)."""
    empty = []
    for sid in spec_ids:
        if _row_count(sid) == 0:
            empty.append(sid)
    assert not empty, f"{len(empty)} asset(s) have 0 rows: {empty[:10]}"


def test_sample_structure_and_string_typing(spec_ids):
    """On a deterministic sample, the SDMX-CSV core columns are present and
    EVERY column is stored as string (SDMX codes must not be coerced to int)."""
    ordered = sorted(spec_ids)
    sample = ordered[:: max(1, len(ordered) // 25)][:25]
    for sid in sample:
        table = load_raw_parquet(sid)
        cols = set(table.column_names)
        missing = _REQUIRED_COLS - cols
        assert not missing, f"{sid}: missing SDMX columns {missing} (cols={sorted(cols)[:12]})"
        non_string = [f.name for f in table.schema if str(f.type) != "string"]
        assert not non_string, f"{sid}: non-string columns {non_string} — codes were coerced"


def test_known_dataflows_above_floor(spec_ids):
    """High-signal series carry the bulk of the value; a sharp row-count drop
    is the clearest signal the bulk path silently degraded."""
    present = set(spec_ids)
    for sid, floor in _FLOORS.items():
        if sid not in present:
            continue
        n = _row_count(sid)
        assert n >= floor, f"{sid}: {n:,} rows < floor {floor:,} — likely truncated"
