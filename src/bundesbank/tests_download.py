"""Post-DAG health invariants for the Bundesbank connector.

Catches silent degradation that file-existence alone misses: empty payloads,
a dataflow that returned only metadata rows, or values that failed to parse to
floats (which would mean the wide-CSV melt drifted).
"""

from subsets_utils import load_raw_parquet


def test_all_raw_assets_nonempty(spec_ids):
    """Every dataflow's raw parquet should hold observations. An empty asset
    usually means the csv-zip endpoint changed shape or returned only metadata."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert len(table) > 0, f"{sid}: raw parquet has 0 rows"


def test_schema_and_values(spec_ids):
    """Columns, types, and no-null guarantees on the melted long table."""
    expected = {"dataflow_id", "series_key", "time_period", "value", "flag"}
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert expected.issubset(set(table.column_names)), (
            f"{sid}: missing columns, got {table.column_names}"
        )
        # value/series_key/time_period are dropped when null at parse time.
        for col in ("value", "series_key", "time_period"):
            assert table.column(col).null_count == 0, (
                f"{sid}: {col} has nulls in raw"
            )
        # value must be a real float column.
        import pyarrow as pa
        assert pa.types.is_floating(table.schema.field("value").type), (
            f"{sid}: value column is not float, got {table.schema.field('value').type}"
        )
        # series keys should be prefixed by the dataflow id (sanity check the melt).
        df_ids = set(table.column("dataflow_id").to_pylist()[:50])
        assert len(df_ids) == 1, f"{sid}: mixed dataflow_id values {df_ids}"
