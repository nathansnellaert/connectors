"""Post-DAG health invariants for the US Census Bureau connector.

Raw assets are NDJSON (list[dict], all string values, per-dataset columns),
loaded through the same subsets_utils loader the download node wrote with.

The corpus is large (1758 datasets), so we sample across the id space rather
than load every asset — per_spec already gates that every asset published;
these checks confirm the sampled raw looks like real Census array-of-arrays
output and not an empty payload or a missing-key HTML body.
"""
from subsets_utils import load_raw_ndjson

_SAMPLE = 40


def _sample(spec_ids):
    ids = sorted(spec_ids)
    if len(ids) <= _SAMPLE:
        return ids
    step = len(ids) / _SAMPLE
    return [ids[int(i * step)] for i in range(_SAMPLE)]


def test_sampled_assets_nonempty_dict_rows(spec_ids):
    """Sampled raw assets must hold >=1 record, each a non-empty header-keyed
    dict. Empty payloads or non-dict rows signal a format switch, auth failure,
    or a non-data body that slipped through the content-type guard."""
    checked = 0
    empty = []
    for sid in _sample(spec_ids):
        try:
            rows = load_raw_ndjson(sid)
        except Exception:
            # Asset absent (per-entity node failure) — surfaced by per_spec,
            # not here. Broad except: R2 misses don't raise FileNotFoundError.
            continue
        checked += 1
        if not rows:
            empty.append(sid)
            continue
        first = rows[0]
        assert isinstance(first, dict) and first, (
            f"{sid}: first row is not a populated dict: {type(first).__name__}"
        )
        assert all(isinstance(k, str) for k in first), (
            f"{sid}: non-string column keys {list(first)[:5]}"
        )
    assert not empty, f"{len(empty)} sampled assets are empty, e.g. {empty[:5]}"
    assert checked > 0, "no sampled assets materialized at all"


def test_no_html_error_body(spec_ids):
    """The missing-key / error responses are HTML, not Census JSON. Confirm
    sampled rows don't carry HTML markers in their column names."""
    for sid in _sample(spec_ids):
        try:
            rows = load_raw_ndjson(sid)
        except Exception:
            continue
        if not rows:
            continue
        keys = " ".join(rows[0].keys()).lower()
        assert "html" not in keys and "<" not in keys, (
            f"{sid}: row keys look like an HTML error body: {list(rows[0])[:5]}"
        )
