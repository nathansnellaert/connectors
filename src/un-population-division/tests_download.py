"""Post-DAG health invariants for the UN Population Division download nodes.

These read raw assets through subsets_utils loaders so they behave identically
locally and on CI. Row counts use Parquet metadata (no full materialization) so
the multi-million-row assets don't get pulled into memory.
"""

import pyarrow.parquet as pq

from subsets_utils import raw_parquet_localpath

# Conservative floors well below observed WPP 2024 volumes; a truncated or
# format-broken download falls far short of these.
_MIN_ROWS = {
    "un-population-division-demographic-indicators": 80_000,
    "un-population-division-fertility-by-age": 1_000_000,
    "un-population-division-life-tables": 1_000_000,
    "un-population-division-migration": 80_000,
    "un-population-division-population-by-age-sex": 1_000_000,
}

# A column each transform depends on — guards against a silent schema swap.
_REQUIRED_COL = {
    "un-population-division-demographic-indicators": "NetMigrations",
    "un-population-division-fertility-by-age": "ASFR",
    "un-population-division-life-tables": "Lx_1",
    "un-population-division-migration": "CNMR",
    "un-population-division-population-by-age-sex": "RefDate",
}


def _download_ids(spec_ids):
    """spec_ids carries every node that ran — downloads AND `-transform` leaves.
    Only downloads write raw parquet, so restrict to the known download set."""
    return [sid for sid in spec_ids if sid in _MIN_ROWS]


def test_raw_assets_have_expected_rows(spec_ids):
    ids = _download_ids(spec_ids)
    assert ids, f"no download spec ids found in {spec_ids}"
    for sid in ids:
        with raw_parquet_localpath(sid) as path:
            meta = pq.ParquetFile(path).metadata
            floor = _MIN_ROWS.get(sid, 1)
            assert meta.num_rows >= floor, (
                f"{sid}: {meta.num_rows} rows < expected floor {floor}"
            )


def test_raw_assets_have_required_columns(spec_ids):
    for sid in _download_ids(spec_ids):
        with raw_parquet_localpath(sid) as path:
            names = set(pq.ParquetFile(path).schema_arrow.names)
        col = _REQUIRED_COL.get(sid)
        if col is not None:
            assert col in names, f"{sid}: missing expected column {col!r} (have {sorted(names)[:12]}...)"
        # Every WPP table is keyed on location-year.
        assert "LocID" in names, f"{sid}: missing LocID"
        assert "Time" in names, f"{sid}: missing Time"
