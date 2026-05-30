"""Post-DAG health invariants for the Bank of Japan download step.

Catches silent degradation that file-existence alone misses: empty corpora,
schema drift, all-null value columns, and bogus periods.
"""

from subsets_utils import load_raw_parquet

EXPECTED_COLUMNS = {
    "db", "series_code", "name", "unit", "frequency", "category",
    "layer1", "layer2", "layer3", "layer4", "layer5",
    "period", "value", "last_update",
}

# DBs known to carry real series. A handful of small/structural DBs could in
# principle enumerate to zero, so we don't require EVERY asset to be non-empty;
# we require the corpus as a whole to be substantial and well-formed.
MIN_NONEMPTY_ASSETS = 40
MIN_TOTAL_ROWS = 500_000


def test_schema_stable(spec_ids):
    """Every asset must carry exactly the declared columns — a changed
    envelope (e.g. API switched key names) shows up here first."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert set(table.column_names) == EXPECTED_COLUMNS, (
            f"{sid}: columns {set(table.column_names)} != expected"
        )


def test_corpus_substantial(spec_ids):
    """The BOJ corpus is ~200k series. Most DBs should hold many observation
    rows; a near-empty corpus means metadata or data calls silently failed."""
    nonempty = 0
    total = 0
    for sid in spec_ids:
        n = len(load_raw_parquet(sid))
        total += n
        if n > 0:
            nonempty += 1
    assert nonempty >= MIN_NONEMPTY_ASSETS, (
        f"only {nonempty}/{len(spec_ids)} assets have rows (want >= {MIN_NONEMPTY_ASSETS})"
    )
    assert total >= MIN_TOTAL_ROWS, (
        f"corpus has only {total} rows (want >= {MIN_TOTAL_ROWS})"
    )


def test_values_and_periods_sane(spec_ids):
    """For non-empty assets, the value column can't be entirely null and
    periods must be plausible YYYY.. integers — guards a transport that
    returns rows of nothing but metadata."""
    import pyarrow.compute as pc

    checked = 0
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        if table.num_rows == 0:
            continue
        checked += 1
        non_null_values = table.num_rows - table.column("value").null_count
        assert non_null_values > 0, f"{sid}: every value is null"

        periods = table.column("period")
        assert periods.null_count == 0, f"{sid}: null period present"
        pmin = pc.min(periods).as_py()
        pmax = pc.max(periods).as_py()
        # Periods are YYYY (4d), YYYYMM (6d), or YYYYMMDD (8d) ints.
        assert 1800 <= pmin, f"{sid}: implausible min period {pmin}"
        assert pmax <= 99999999, f"{sid}: implausible max period {pmax}"

        # series_code must be populated on every row.
        assert table.column("series_code").null_count == 0, (
            f"{sid}: null series_code present"
        )
    assert checked > 0, "no non-empty assets to validate"
