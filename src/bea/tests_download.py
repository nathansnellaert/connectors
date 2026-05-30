"""Health-invariant tests for the BEA download step.

Run post-DAG, in-process, through subsets_utils loaders so behaviour is
identical locally and on CI. Catches silent degradation that file-existence
alone misses: empty payloads, truncated ZIPs, an endpoint that switched format
or auth that expired (the BEA API returns a structured error envelope with zero
Data rows in that case).
"""
from subsets_utils import load_raw_ndjson, load_raw_file, list_raw_files

from nodes.bea import ID_TO_DATASET, DATASET_STRATEGY

# Floor row counts per REST dataset — every BEA dataset publishes far more than
# this; anything at/under the floor means a degraded fetch, not real sparsity.
_REST_MIN_ROWS = 100


def test_rest_assets_have_rows(spec_ids):
    """Every REST-backed spec's NDJSON asset holds a healthy number of rows."""
    for sid in spec_ids:
        dataset = ID_TO_DATASET[sid]
        if DATASET_STRATEGY[dataset]["mode"] == "regional_zip":
            continue
        rows = load_raw_ndjson(sid)
        assert len(rows) >= _REST_MIN_ROWS, (
            f"{sid} ({dataset}): only {len(rows)} rows (< {_REST_MIN_ROWS}) - "
            "likely an error envelope or truncated fetch"
        )
        # A real BEA Data row always carries a value field.
        assert "DataValue" in rows[0], (
            f"{sid} ({dataset}): rows missing DataValue, got keys {list(rows[0])}"
        )


def test_regional_zips_present_and_nonempty(spec_ids):
    """The Regional spec writes one ZIP per series; each must be a real archive."""
    for sid in spec_ids:
        dataset = ID_TO_DATASET[sid]
        if DATASET_STRATEGY[dataset]["mode"] != "regional_zip":
            continue
        files = list_raw_files(f"{sid}-*.zip")
        assert files, f"{sid}: no Regional series ZIPs were written"
        for rel in files:
            asset = rel.rsplit("/", 1)[-1][: -len(".zip")]
            content = load_raw_file(asset, extension="zip", binary=True)
            assert len(content) > 1000, f"{asset}: ZIP is {len(content)} bytes (truncated?)"
            assert content[:2] == b"PK", f"{asset}: not a ZIP archive"
