"""Health invariants for the Bank of Japan connector.

Catch silent degradation the harness's file-existence check misses: empty
payloads, a DB that returned metadata but no observations, or a schema/format
switch that drops the value column.
"""

import pyarrow.compute as pc

from subsets_utils import load_raw_parquet

EXPECTED_COLS = {
    "series_code", "name", "unit", "frequency",
    "category", "last_update", "period", "value",
}


def _download_ids(spec_ids):
    """Only download specs write raw parquet. Transform specs (``-transform``)
    publish Delta tables and have no raw asset — exclude them so the loaders
    don't look for a parquet that never exists."""
    return [s for s in spec_ids if not s.endswith("-transform")]


def test_all_raw_assets_nonempty(spec_ids):
    """Every DB's raw parquet must hold observation rows. An empty payload
    usually means the API changed format or the metadata enumeration broke."""
    for sid in _download_ids(spec_ids):
        table = load_raw_parquet(sid)
        assert len(table) > 0, f"{sid}: raw parquet has 0 rows"


def test_raw_schema_and_values(spec_ids):
    """Schema must carry the observation columns, and most rows must have a
    real numeric value and a parseable period (not all-null)."""
    for sid in _download_ids(spec_ids):
        table = load_raw_parquet(sid)
        cols = set(table.column_names)
        assert EXPECTED_COLS <= cols, f"{sid}: missing columns {EXPECTED_COLS - cols}"

        non_null_values = pc.count(table.column("value"), mode="only_valid").as_py()
        assert non_null_values > 0, f"{sid}: every value is null"

        # series_code must always be populated.
        null_codes = pc.count(table.column("series_code"), mode="only_null").as_py()
        assert null_codes == 0, f"{sid}: {null_codes} rows with null series_code"

        # Period must be populated for the overwhelming majority of rows. The
        # source sporadically reports an observation with a value but an empty
        # survey date (e.g. ~1.5k of CO/TANKAN's tens of millions of rows); raw
        # keeps these faithfully and the transform drops them. A *high* null
        # rate, by contrast, signals a real format break worth failing on.
        n = table.num_rows
        null_periods = pc.count(table.column("period"), mode="only_null").as_py()
        assert null_periods / n < 0.01, (
            f"{sid}: {null_periods}/{n} rows ({null_periods / n:.1%}) have a "
            f"null period — exceeds 1% tolerance for source malformations"
        )
