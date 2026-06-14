"""Post-DAG health invariants for the BTS Socrata catalog.

These run in-process after the connector DAG, loading raw through the same
subsets_utils loader the download node used (gzip'd NDJSON).
"""

from subsets_utils import load_raw_ndjson


def test_all_raw_assets_nonempty(spec_ids):
    """Every dataset's raw NDJSON should hold rows. An empty payload usually
    means the resource endpoint changed format or the dataset was retired."""
    empty = []
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        if not rows:
            empty.append(sid)
    assert not empty, f"raw NDJSON empty for: {empty[:10]}"


def test_rows_are_dicts_with_columns(spec_ids):
    """Each row is a flat record projected onto the documented column set —
    a non-empty dict. Catches a writer that emitted bare strings or [] rows."""
    bad = []
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        first = rows[0]
        if not isinstance(first, dict) or len(first) == 0:
            bad.append((sid, type(first).__name__))
    assert not bad, f"first row not a populated dict for: {bad[:10]}"
