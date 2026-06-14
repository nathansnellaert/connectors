"""Post-DAG health invariants for the UNODC connector.

Catches silent degradation that file-existence alone misses: empty/truncated
workbooks, a portal URL that started returning an error page, a parser that
matched the wrong header row and produced all-null columns.
"""

from subsets_utils import load_raw_parquet

# Country profile publishes the M49 reference table (no observation value).
_REFERENCE_ID = "unodc-21d5221e-430c-449e-aed3-0271822050d9"

# Lower bounds well under observed counts (homicide ~121k, glotip ~65k, sdg ~63k,
# seizures ~11k, treatment ~24k, wildlife ~94, m49 ~243). Tiny themes exist, so the
# floor is deliberately low; it only guards against empty/truncated payloads.
_MIN_ROWS = 50

_EXPECTED_COLS = {
    "iso3", "geo", "region", "subregion", "indicator", "series", "dimension",
    "category", "sex", "age", "drug", "year", "unit", "value", "value_text", "source",
}


def test_all_raw_assets_nonempty(spec_ids):
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert len(table) >= _MIN_ROWS, f"{sid}: only {len(table)} rows (<{_MIN_ROWS})"


def test_schema_is_universal(spec_ids):
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert set(table.column_names) == _EXPECTED_COLS, (
            f"{sid}: columns {table.column_names} != universal schema"
        )


def test_geo_populated(spec_ids):
    """Every row carries a geography label — a wrong header row would null it out."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        nonnull = table.column("geo").combine_chunks().is_valid().true_count
        assert nonnull == len(table), f"{sid}: {len(table) - nonnull} rows missing geo"


def test_value_themes_have_numeric_values(spec_ids):
    """Valued themes (everything but the reference table) must carry real numeric
    observations; an all-null value column means the value header was misread."""
    for sid in spec_ids:
        if sid == _REFERENCE_ID:
            continue
        table = load_raw_parquet(sid)
        valid = table.column("value").combine_chunks().is_valid().true_count
        assert valid > 0, f"{sid}: value column is entirely null"


def test_year_present_for_value_themes(spec_ids):
    for sid in spec_ids:
        if sid == _REFERENCE_ID:
            continue
        table = load_raw_parquet(sid)
        years = table.column("year").combine_chunks()
        valid = years.is_valid().true_count
        assert valid > 0, f"{sid}: year column is entirely null"
