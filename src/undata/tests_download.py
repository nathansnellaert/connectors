"""Health-invariant tests for the UNdata download nodes.

Run post-DAG, in-connector. Catch silent degradation that file-existence
alone misses: empty payloads, columns dropped, all-null values, or the SDMX
endpoint quietly switching format.
"""
from subsets_utils import load_raw_parquet

EXPECTED_COLS = {"dataflow", "series_key", "time_period", "obs_value"}


def test_all_raw_assets_nonempty(spec_ids):
    """Every dataflow should yield observation rows. Empty usually means the
    SDMX endpoint changed format or the Accept header stopped being honoured."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert len(table) > 0, f"{sid}: raw parquet has 0 rows"


def test_schema_and_values(spec_ids):
    """Uniform schema everywhere, and most observations carry a numeric value.
    A wholesale null OBS_VALUE column means parsing latched onto the wrong
    column."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        cols = set(table.column_names)
        assert EXPECTED_COLS <= cols, f"{sid}: missing columns, got {cols}"
        non_null = table.column("obs_value").null_count
        frac_valued = 1 - (non_null / len(table))
        assert frac_valued > 0.5, (
            f"{sid}: only {frac_valued:.1%} of rows have a numeric obs_value"
        )
        # series_key must actually encode dimensions (non-empty for most rows).
        empty_keys = sum(1 for v in table.column("series_key").to_pylist() if not v)
        assert empty_keys < len(table), f"{sid}: every series_key is empty"
