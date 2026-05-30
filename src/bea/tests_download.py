"""Health-invariant tests for the BEA download step.

Each REST dataset writes one or more NDJSON batches named
`bea-<entity>-<batch>.ndjson.zst`; the Regional dataset writes opaque
`bea-regional-<code>.zip` files. Coverage per spec is bounded by a wall-clock
budget, so we assert that EVERY spec produced at least one non-empty batch
(empty payloads usually mean the endpoint switched format or auth expired)
rather than asserting a fixed corpus size.
"""
import os

from subsets_utils import list_raw_files, load_raw_file, load_raw_ndjson

_NDJSON_SUFFIX = ".ndjson.zst"
_REGIONAL = "bea-regional"


def _basename_asset(path: str, suffix: str) -> str:
    name = os.path.basename(path)
    assert name.endswith(suffix), f"unexpected raw file name: {path}"
    return name[: -len(suffix)]


def test_every_spec_wrote_batches(spec_ids):
    """Every download spec must have produced at least one raw batch file.

    A spec with zero batches means its driving-parameter walk returned nothing
    or every GetData call errored — a silent break we want to surface."""
    for sid in spec_ids:
        pattern = f"{sid}-*.zip" if sid == _REGIONAL else f"{sid}-*{_NDJSON_SUFFIX}"
        files = list_raw_files(pattern)
        assert files, f"{sid}: no raw batch files written ({pattern})"


def test_ndjson_batches_have_rows(spec_ids):
    """REST-dataset batches must be non-empty long-format records."""
    for sid in spec_ids:
        if sid == _REGIONAL:
            continue
        files = list_raw_files(f"{sid}-*{_NDJSON_SUFFIX}")
        assert files, f"{sid}: no NDJSON batches"
        asset = _basename_asset(files[0], _NDJSON_SUFFIX)
        rows = load_raw_ndjson(asset)
        assert len(rows) > 0, f"{sid}: first batch {asset} has 0 rows"
        assert isinstance(rows[0], dict), f"{sid}: batch row is not a dict"
        # BEA GetData rows always carry a DataValue field.
        assert "DataValue" in rows[0], f"{sid}: row missing DataValue: {list(rows[0])[:8]}"


def test_regional_zips_are_valid(spec_ids):
    """Regional bulk ZIPs must be real (PK magic) and non-trivial in size."""
    if _REGIONAL not in spec_ids:
        return
    files = list_raw_files(f"{_REGIONAL}-*.zip")
    assert len(files) >= 5, f"bea-regional: expected >=5 ZIPs, got {len(files)}"
    asset = _basename_asset(files[0], ".zip")
    content = load_raw_file(asset, extension="zip", binary=True)
    assert content[:2] == b"PK", f"{asset}: not a valid ZIP (no PK magic)"
    assert len(content) > 10_000, f"{asset}: ZIP suspiciously small ({len(content)} bytes)"
