"""Health-invariant tests for the GitHub Innovation Graph download.

These run in-process after the DAG, seeing raw data through the same
subsets_utils loaders the download node wrote with. They catch silent
degradation that mere file existence misses: empty/truncated payloads, a
metric file that switched format, or the year/quarter partition columns
vanishing.
"""

from subsets_utils import load_raw_parquet

# Columns every Innovation Graph metric file carries (long-format, partitioned
# internally by year/quarter). economy_collaborators uses source/destination
# pair columns instead of iso2_code, so we only assert the universal pair.
_REQUIRED_COLUMNS = {"year", "quarter"}

# Smallest real file (organizations/git_pushes) has a few thousand rows; the
# corpus starts in 2020 Q1 and grows quarterly, so every metric is well over
# this floor. A download truncated to a header-only stub would trip it.
_MIN_ROWS = 1000


def test_all_raw_assets_nonempty(spec_ids):
    """Every spec's raw parquet should hold a substantial number of rows."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert table.num_rows >= _MIN_ROWS, (
            f"{sid}: raw parquet has {table.num_rows} rows (< {_MIN_ROWS}) "
            "- likely a truncated or format-switched download"
        )


def test_partition_columns_present(spec_ids):
    """Every metric file must keep its year/quarter partition columns, and the
    partition values must be sane (year >= 2020, quarter in 1..4)."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        cols = set(table.column_names)
        missing = _REQUIRED_COLUMNS - cols
        assert not missing, f"{sid}: missing partition columns {missing} (have {cols})"

        years = table.column("year").to_pylist()
        quarters = table.column("quarter").to_pylist()
        assert min(years) >= 2020, f"{sid}: found year {min(years)} < 2020"
        assert set(quarters) <= {1, 2, 3, 4}, (
            f"{sid}: quarter values out of range: {sorted(set(quarters))}"
        )
