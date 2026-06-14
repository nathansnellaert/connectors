"""Post-DAG health invariants for the Bank of Canada connector.

Raw is read back through subsets_utils loaders (never the filesystem) so these
behave identically locally and on CI / R2.
"""

from subsets_utils import list_raw_files, load_raw_parquet


def test_series_catalog_nonempty():
    """The series catalog should hold thousands of rows with real ids/labels."""
    table = load_raw_parquet("bank-of-canada-series")
    assert len(table) > 1000, f"series catalog only {len(table)} rows (expected >1000)"
    sids = table.column("series_id").to_pylist()
    assert all(s for s in sids), "series catalog has empty series_id values"
    labels = table.column("label").to_pylist()
    assert any(lbl for lbl in labels), "series catalog has no labels at all"


def test_values_batches_present_and_nonempty():
    """Values are written as bucket batches; expect many files and real rows."""
    files = list_raw_files("bank-of-canada-values-bucket-*")
    assert len(files) > 50, f"only {len(files)} value bucket files (expected many)"

    total = 0
    sampled = 0
    for rel in files[:5]:
        asset = rel.split("/")[-1].split(".")[0]
        table = load_raw_parquet(asset)
        total += len(table)
        sampled += 1
        for col in ("series_id", "date", "value"):
            assert col in table.column_names, f"{asset}: missing column {col}"
    assert total > 0, f"first {sampled} value buckets all empty"


def test_values_dates_look_iso():
    """Observation dates should be ISO 'YYYY-MM-DD' strings, not garbage."""
    files = list_raw_files("bank-of-canada-values-bucket-*")
    assert files, "no value bucket files found"
    asset = files[0].split("/")[-1].split(".")[0]
    table = load_raw_parquet(asset)
    dates = [d for d in table.column("date").to_pylist() if d]
    assert dates, f"{asset}: no dates present"
    sample = dates[0]
    assert len(sample) == 10 and sample[4] == "-" and sample[7] == "-", (
        f"unexpected date format: {sample!r}"
    )
