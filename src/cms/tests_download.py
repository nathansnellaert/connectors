"""Health-invariant tests for the CMS download step.

Raw is written as NDJSON (zstd) by ``fetch_one`` for every spec, regardless of portal
(main data-api / medicaid + provider DKAN). These tests load it back through the same
``subsets_utils`` loader and check for silent degradation: missing coverage, assets that
fail to load, and a corpus-wide collapse to empty payloads (endpoint format change / auth
or pagination breakage). A handful of legitimately tiny/empty datasets is tolerated.
"""
from subsets_utils import load_raw_ndjson


def test_specs_cover_cms(spec_ids):
    """Every spec id that ran is a well-formed CMS download id."""
    assert spec_ids, "no download spec ids ran"
    bad = [s for s in spec_ids if not s.startswith("cms-")]
    assert not bad, f"unexpected spec ids: {bad[:5]}"


def _sample(spec_ids, k=24):
    s = sorted(spec_ids)
    if len(s) <= k:
        return s
    step = len(s) / k
    return [s[int(i * step)] for i in range(k)]


def test_raw_assets_loadable(spec_ids):
    """A spread of assets across portals must load as NDJSON rows of dicts.

    Sorted striding draws from all three portal prefixes (cms-main / cms-medicaid /
    cms-provider). load_raw_ndjson raises if an asset is missing or corrupt.
    """
    for sid in _sample(spec_ids):
        rows = load_raw_ndjson(sid)
        if rows:
            assert isinstance(rows[0], dict), f"{sid}: row is not a JSON object"


def test_corpus_not_all_empty(spec_ids):
    """The CMS datasets in this union are non-empty in practice. The vast majority of a
    sampled spread must carry rows; allow up to ~10% legitimately tiny/empty datasets."""
    sample = _sample(spec_ids)
    empties = [sid for sid in sample if not load_raw_ndjson(sid)]
    assert len(empties) <= max(1, len(sample) // 10), (
        f"{len(empties)}/{len(sample)} sampled assets empty -- likely a portal-wide "
        f"fetch failure: {empties[:8]}"
    )
