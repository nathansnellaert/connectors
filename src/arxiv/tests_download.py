"""Health invariants for the arXiv OAI-PMH download.

Raw is written as one-or-more NDJSON batch files per run, named
`arxiv-papers-<run_ts>-<seq>.ndjson.zst`. The corpus is huge (~2.5M records
harvested across many bounded runs), so these tests are O(1) in corpus size:
they discover batches via list_raw_files and load AT MOST ONE batch, asserting
the harvest produced real, well-formed arXiv-format metadata — catching silent
degradation (empty payloads, a format switch to oai_dc, an all-deleted page)
that a file-existence check would miss.
"""
from subsets_utils import list_raw_files, load_raw_ndjson

_BATCH_GLOB = "arxiv-papers-*.ndjson*"


def _batch_asset_ids() -> list[str]:
    ids = []
    for path in list_raw_files(_BATCH_GLOB):
        asset = path.split("/")[-1]
        for ext in (".ndjson.zst", ".ndjson.gz", ".ndjson"):
            if asset.endswith(ext):
                asset = asset[: -len(ext)]
                break
        ids.append(asset)
    return ids


def test_batches_exist():
    """At least one batch file must have been written. None => the endpoint
    never warmed / the spec failed to harvest anything this run."""
    assert _batch_asset_ids(), "no arxiv-papers-* raw batch files were written"


def test_first_batch_has_records():
    """The first batch should hold a full page-worth of records. A single OAI
    page is ~1300 records; even a budget-truncated run flushes >= 1000. An empty
    or tiny batch means the endpoint switched format or returned an error page."""
    assets = _batch_asset_ids()
    assert assets, "no batches to check"
    rows = load_raw_ndjson(assets[0])
    assert len(rows) >= 1000, (
        f"{assets[0]}: only {len(rows)} records; expected >= 1000 "
        f"(one OAI page is ~1300)")


def test_records_have_core_fields():
    """Live (non-deleted) records must carry an arxiv_id and a datestamp, and
    the vast majority must have a title — the metadataPrefix=arXiv contract. A
    silent fallback to oai_dc, or a header-only/all-deleted page, breaks these."""
    assets = _batch_asset_ids()
    assert assets, "no batches to check"
    rows = load_raw_ndjson(assets[0])

    live = [r for r in rows if not r.get("deleted")]
    assert live, f"{assets[0]}: every record is flagged deleted — degenerate harvest"

    missing_id = [r for r in live if not r.get("arxiv_id")]
    assert not missing_id, f"{len(missing_id)}/{len(live)} live records missing arxiv_id"
    missing_ds = [r for r in live if not r.get("datestamp")]
    assert not missing_ds, f"{len(missing_ds)}/{len(live)} live records missing datestamp"

    with_title = sum(1 for r in live if r.get("title"))
    assert with_title / len(live) >= 0.95, (
        f"only {with_title}/{len(live)} live records have a title — "
        "metadata format may have changed")


def test_records_have_arxiv_format_richness():
    """The arXiv prefix (vs oai_dc) exposes structured authors. Assert a strong
    majority of live records carry a parsed authors list of dicts — the signal
    that we are on the rich 'arXiv' format and the nested parse worked, not a
    flattened Dublin Core string."""
    assets = _batch_asset_ids()
    assert assets, "no batches to check"
    rows = load_raw_ndjson(assets[0])
    live = [r for r in rows if not r.get("deleted")]
    assert live, "no live records"

    with_authors = [r for r in live if r.get("authors")]
    assert len(with_authors) / len(live) >= 0.9, (
        f"only {len(with_authors)}/{len(live)} live records have structured "
        "authors — expected the rich arXiv metadata format")
    sample = with_authors[0]["authors"]
    assert isinstance(sample, list) and isinstance(sample[0], dict), \
        "authors is not a list[dict] — nested parse failed"
    assert "keyname" in sample[0], "author entries missing keyname"
