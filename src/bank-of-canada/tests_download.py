"""Health invariants for the Bank of Canada download nodes.

Catches silent degradation that file-existence checks miss: an empty catalog,
a values asset that came back with no observations, or columns lost to a format
switch.
"""

import pyarrow.compute as pc

from subsets_utils import load_raw_parquet


def test_raw_assets_nonempty(spec_ids):
    """Every download asset must hold rows."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert len(table) > 0, f"{sid}: raw parquet has 0 rows"


def test_series_catalog(spec_ids):
    """The series catalog should list thousands of series with stable columns."""
    table = load_raw_parquet("bank-of-canada-series")
    assert set(table.column_names) >= {"series_id", "label", "description", "link"}, (
        f"series columns drifted: {table.column_names}"
    )
    assert len(table) > 5000, f"series catalog only {len(table)} rows (expected ~15k)"
    assert pc.sum(pc.is_null(table["series_id"])).as_py() == 0, "null series_id in catalog"


def test_values_observations(spec_ids):
    """The observations asset should carry the long-format shape with volume."""
    table = load_raw_parquet("bank-of-canada-values")
    assert set(table.column_names) >= {"series_id", "date", "value"}, (
        f"values columns drifted: {table.column_names}"
    )
    # 15.6k series of full history is millions of rows; 100k is a safe floor
    # that still trips on a truncated/partial download.
    assert len(table) > 100_000, f"values only {len(table)} rows (expected millions)"
    assert pc.sum(pc.is_null(table["series_id"])).as_py() == 0, "null series_id in values"
    assert pc.sum(pc.is_null(table["date"])).as_py() == 0, "null date in values"
