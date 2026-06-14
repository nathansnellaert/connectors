"""Post-DAG health invariants for the BEA download nodes.

Run in-connector after the DAG, through subsets_utils loaders, so they behave
identically locally and on CI. Each BEA dataset writes one gzip ndjson asset
(<download_id>.ndjson.gz); we stream-read rather than load_raw_ndjson to avoid
pulling multi-million-row datasets (ITA/IIP/InputOutput) into memory.

`spec_ids` may include the SQL transform ids (which end in "-transform" and
publish Delta tables, NOT ndjson raw). These tests probe the raw download
layer, so we filter to the download ids only.
"""
import json

from subsets_utils import list_raw_files, raw_reader


def _download_ids(spec_ids):
    """Download nodes write ndjson raw; transform leaves publish Delta tables.
    Only the download ids have a <id>.ndjson.gz raw asset to read."""
    return [sid for sid in spec_ids if not sid.endswith("-transform")]


def test_every_spec_wrote_rows(spec_ids):
    """Every dataset node must leave a non-empty ndjson asset. An empty payload
    means the catalog walk broke, auth lapsed, or GetData changed contract."""
    download_ids = _download_ids(spec_ids)
    assert download_ids, "no download spec ids present"
    for sid in download_ids:
        files = list_raw_files(f"{sid}.*")
        assert files, f"{sid}: no raw file written"
        with raw_reader(sid, "ndjson.gz", mode="rt", compression="gzip") as f:
            first = f.readline()
        assert first.strip(), f"{sid}: raw ndjson has no rows"
        row = json.loads(first)
        assert isinstance(row, dict) and row, f"{sid}: first row is not a JSON object"


def test_datavalue_present(spec_ids):
    """Every BEA observation row carries a DataValue field (the measured cell).
    Its absence on the first row signals a parse/format regression."""
    for sid in _download_ids(spec_ids):
        with raw_reader(sid, "ndjson.gz", mode="rt", compression="gzip") as f:
            first = f.readline()
        row = json.loads(first)
        # MNE carries the numeric under DataValueUnformatted; all others DataValue.
        assert "DataValue" in row or "DataValueUnformatted" in row, (
            f"{sid}: first row missing DataValue/DataValueUnformatted: {sorted(row)[:8]}"
        )
