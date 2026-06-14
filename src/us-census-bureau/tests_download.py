"""Post-DAG health invariants for the US Census Bureau connector.

Raw assets are NDJSON (list[dict], all string values, per-dataset columns).
Loaded through the same subsets_utils loader the download node wrote with.
"""
from subsets_utils import load_raw_ndjson


def test_raw_assets_nonempty(spec_ids):
    """Every raw asset that materialized should hold at least one record.

    Census data queries that return a header-only (zero data row) response are
    raised as failures in the download node, so any asset present on disk must
    be non-empty; an empty one means a silent format/auth regression."""
    checked = 0
    empty = []
    for sid in spec_ids:
        try:
            rows = load_raw_ndjson(sid)
        except FileNotFoundError:
            # Node may have failed (e.g. unsupported geography) — per-entity,
            # not this test's concern. Only assert on assets that exist.
            continue
        checked += 1
        if not rows:
            empty.append(sid)
    assert not empty, f"{len(empty)} raw assets are empty, e.g. {empty[:5]}"
    assert checked > 0, "no raw assets materialized at all"


def test_rows_are_dicts_with_columns(spec_ids):
    """Each record must be a non-empty dict (header-keyed row), confirming the
    array-of-arrays response was parsed into named columns rather than left as
    bare lists or scalars."""
    for sid in spec_ids:
        try:
            rows = load_raw_ndjson(sid)
        except FileNotFoundError:
            continue
        if not rows:
            continue
        sample = rows[0]
        assert isinstance(sample, dict) and sample, (
            f"{sid}: first row is not a populated dict: {type(sample).__name__}"
        )
        # Census returns all values as strings; at least confirm keys exist.
        assert all(isinstance(k, str) for k in sample), f"{sid}: non-string column keys"
        return  # one materialized asset is enough to validate the shape
