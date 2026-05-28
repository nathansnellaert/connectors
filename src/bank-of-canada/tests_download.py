"""Health invariants for the Bank of Canada download.

`series` is a full snapshot (always complete). `values` is a cursor-paced
firehose, so a single run may only have backfilled part of the ~15.5k series —
tests assert "some well-formed observations landed", not full coverage.
"""

from subsets_utils import list_raw_files, load_raw_parquet

_VALUES_GLOB = "bank-of-canada-values-*.parquet"
_SERIES_COLS = {"series_id", "label", "description", "link"}
_VALUES_COLS = {"series_id", "date", "value"}


def test_series_catalog_nonempty():
    """The series catalog should hold ~15.5k rows; a tiny count means the
    listing endpoint changed shape or returned an error envelope."""
    t = load_raw_parquet("bank-of-canada-series")
    assert len(t) > 10_000, f"series catalog has only {len(t)} rows (expected ~15.5k)"
    assert _SERIES_COLS <= set(t.column_names), \
        f"series columns {t.column_names} missing {_SERIES_COLS}"


def test_series_ids_unique_and_present():
    t = load_raw_parquet("bank-of-canada-series")
    ids = t.column("series_id").to_pylist()
    assert all(ids), "null/empty series_id in catalog"
    assert len(ids) == len(set(ids)), "duplicate series_id in catalog"


def test_values_batches_present():
    """The firehose writes one parquet per flush window; none means the
    observations crawl produced nothing."""
    assert list_raw_files(_VALUES_GLOB), "no values batch files written"


def test_values_rows_wellformed():
    """Observations must un-pivot to (series_id, date, value) long rows with
    ISO dates and float values — guards against a format/parse regression."""
    files = list_raw_files(_VALUES_GLOB)
    total = 0
    checked_structure = False
    for f in files[:10]:                       # sample to bound test time
        table = load_raw_parquet(f[: -len(".parquet")])
        total += len(table)
        if len(table) and not checked_structure:
            assert _VALUES_COLS == set(table.column_names), \
                f"{f} columns {table.column_names} != {_VALUES_COLS}"
            assert all(table.column("series_id").to_pylist()[:20]), "null series_id"
            for d in table.column("date").to_pylist()[:20]:
                assert isinstance(d, str) and len(d) == 10 and d[4] == "-", f"bad date {d!r}"
            for v in table.column("value").to_pylist()[:50]:
                assert isinstance(v, float), f"non-float value {v!r}"
            checked_structure = True
    assert total > 0, "all sampled values batches are empty"
