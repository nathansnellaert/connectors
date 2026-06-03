"""Health invariants for the Gapminder download step.

Each spec writes one long-format parquet folding a whole open-numbers repo's
datapoint CSVs (geo, time, indicator, value, dimension, extra_dims). These
tests catch silent degradation a file-existence check would miss: truncated
crawls (too few rows / indicators), wrong schema, or all-null values.
"""
from subsets_utils import load_raw_parquet

EXPECTED_COLUMNS = {"geo", "time", "indicator", "value", "dimension", "extra_dims"}

# Floors well below observed reality (~480 indicators / millions of rows for
# systema_globalis; ~230 / hundreds of thousands for fasttrack). Tripping these
# means the crawl was truncated, not that the source shrank a little.
MIN_ROWS = 50_000
MIN_INDICATORS = 100


def test_schema_and_nonempty(spec_ids):
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert set(table.column_names) == EXPECTED_COLUMNS, (
            f"{sid}: columns {table.column_names} != {EXPECTED_COLUMNS}"
        )
        assert table.num_rows >= MIN_ROWS, (
            f"{sid}: {table.num_rows} rows < {MIN_ROWS} (truncated crawl?)"
        )


def test_indicator_coverage(spec_ids):
    """Each repo folds hundreds of distinct indicators into one table."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        n_ind = len(set(table.column("indicator").to_pylist()))
        assert n_ind >= MIN_INDICATORS, (
            f"{sid}: only {n_ind} distinct indicators < {MIN_INDICATORS}"
        )


def test_keys_present(spec_ids):
    """geo / time / indicator are the natural key — none may be null."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        for col in ("geo", "time", "indicator"):
            assert table.column(col).null_count == 0, f"{sid}: nulls in {col}"
        # At least some values must be non-null — an all-null value column means
        # the value field was mis-mapped during parsing.
        assert table.column("value").null_count < table.num_rows, (
            f"{sid}: every value is null — value column mis-mapped"
        )
