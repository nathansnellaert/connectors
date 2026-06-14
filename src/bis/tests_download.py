"""Health-invariant tests for the BIS bulk-CSV connector.

Each download asset is a zstd-compressed parquet of SDMX observations, with
every column stored as VARCHAR. WS_LBS_D_PUB is multi-GB, so the tests read the
parquet footer (row count + schema) and at most a single row group rather than
loading whole assets into memory.
"""

import pyarrow.parquet as pq

from subsets_utils import list_raw_files, raw_reader


def test_all_raw_assets_present(spec_ids):
    """Every download spec should have produced a raw parquet file. A missing
    file means the bulk zip vanished or the fetch crashed silently."""
    missing = [sid for sid in spec_ids if not list_raw_files(f"{sid}.parquet")]
    assert not missing, f"raw parquet missing for: {missing}"


def test_raw_assets_nonempty(spec_ids):
    """Every asset must hold observations. Empty payloads usually mean the
    endpoint switched format or returned the wide (_csv_col) layout."""
    for sid in spec_ids:
        with raw_reader(sid, "parquet", mode="rb") as fh:
            n = pq.ParquetFile(fh).metadata.num_rows
        assert n > 0, f"{sid}: raw parquet has 0 rows"


def test_raw_assets_have_expected_columns(spec_ids):
    """Each asset must carry OBS_VALUE and TIME_PERIOD (every BIS dataflow has
    them), and a sampled batch's OBS_VALUE must be numeric — guards against the
    endpoint switching format or a parser regression that shifts columns."""
    for sid in spec_ids:
        with raw_reader(sid, "parquet", mode="rb") as fh:
            pf = pq.ParquetFile(fh)
            names = set(pf.schema_arrow.names)
            assert "OBS_VALUE" in names, f"{sid}: no OBS_VALUE column, cols={sorted(names)[:8]}"
            assert "TIME_PERIOD" in names, f"{sid}: no TIME_PERIOD column, cols={sorted(names)[:8]}"
            # Sample the first row group only.
            batch = next(pf.iter_batches(batch_size=2000, columns=["OBS_VALUE"]))
            vals = batch.column("OBS_VALUE").to_pylist()
        sample = [v for v in vals if v is not None][:500]
        assert sample, f"{sid}: first 2000 rows all have null OBS_VALUE"
        bad = [v for v in sample if not _is_number(v)]
        assert not bad, f"{sid}: non-numeric OBS_VALUE samples: {bad[:5]}"


def _is_number(v) -> bool:
    if v is None or v == "":
        return False
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False
