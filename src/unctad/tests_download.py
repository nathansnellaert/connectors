"""Health invariants for UNCTAD raw assets.

Each raw asset is a uniform long-format parquet (one row per observation). We
check row count and schema via parquet metadata so multi-GB assets (e.g.
US.TradeMatrix) don't get loaded into memory.
"""
import pyarrow.parquet as pq

from subsets_utils import raw_parquet_localpath

_EXPECTED_COLS = {
    "period", "dimensions", "measure", "value_raw", "footnote", "missing_value",
}


def test_all_raw_assets_nonempty_and_well_formed(spec_ids):
    """Every report's raw parquet must hold observations and the long schema.

    Empty payloads usually mean the bulk endpoint changed or decompression
    silently produced nothing; a missing column means the wide->long parse
    drifted.
    """
    for sid in spec_ids:
        with raw_parquet_localpath(sid) as path:
            md = pq.ParquetFile(path)
            n = md.metadata.num_rows
            cols = set(md.schema_arrow.names)
        assert n > 0, f"{sid}: raw parquet has 0 rows"
        missing = _EXPECTED_COLS - cols
        assert not missing, f"{sid}: raw parquet missing columns {missing}"


def test_values_present(spec_ids):
    """At least one asset's first row group should carry a non-empty value cell.

    Catches the degenerate case where every measure cell parsed empty (which
    would make every transform return 0 rows and fail the publish).
    """
    for sid in spec_ids:
        with raw_parquet_localpath(sid) as path:
            pf = pq.ParquetFile(path)
            batch = next(pf.iter_batches(batch_size=2048, columns=["value_raw"]))
            vals = [v for v in batch.column("value_raw").to_pylist() if v and v.strip()]
        assert vals, f"{sid}: no non-empty value_raw in first row group"
