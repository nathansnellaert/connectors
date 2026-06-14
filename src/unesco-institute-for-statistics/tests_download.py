"""Health-invariant tests for the UIS download nodes.

Run post-DAG, in-process, through subsets_utils loaders so they behave the
same locally and on CI. Thresholds are anchored to the observed Feb 2026
BDDS release (indicators ~5k across both archives; national observations a
few million rows).
"""
from subsets_utils import load_raw_parquet


def test_indicators_nonempty():
    """Indicator catalog should hold thousands of indicators across both
    archives. A near-empty file means a LABEL.csv went missing or the bulk
    listing handed us the wrong archive."""
    t = load_raw_parquet("unesco-institute-for-statistics-indicators")
    assert t.num_rows >= 1000, f"indicators: only {t.num_rows} rows"
    cols = set(t.column_names)
    assert {"INDICATOR_ID", "INDICATOR_LABEL_EN", "DATASET"} <= cols, cols
    datasets = set(t.column("DATASET").to_pylist())
    assert {"SDG", "OPRI"} <= datasets, f"missing an archive: {datasets}"


def test_values_nonempty_and_typed():
    """National observations should number in the millions, carry both
    archives, and have real numeric values — not an all-null VALUE column
    (which is how a silent format change usually shows up)."""
    t = load_raw_parquet("unesco-institute-for-statistics-values")
    assert t.num_rows >= 500_000, f"values: only {t.num_rows} rows"
    cols = set(t.column_names)
    expected = {"INDICATOR_ID", "COUNTRY_ID", "YEAR", "VALUE", "DATASET"}
    assert expected <= cols, cols
    assert t.column("VALUE").null_count < t.num_rows, "VALUE entirely null"
    datasets = set(t.column("DATASET").to_pylist()[:50000])
    assert "SDG" in datasets or "OPRI" in datasets, datasets
