"""Health invariants for the arXiv OAI-PMH download.

Raw is written as one-or-more NDJSON batch files per run, named
`arxiv-papers-<run_ts>-<seq>`. We discover them via list_raw_files and load the
union, then assert the harvest produced real, well-formed metadata records —
catching silent degradation (empty payloads, format switch, all-deleted) that a
file-existence check would miss.
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


def _load_all_papers() -> list[dict]:
    rows: list[dict] = []
    for asset in _batch_asset_ids():
        rows.extend(load_raw_ndjson(asset))
    return rows


def test_papers_batches_exist_and_nonempty():
    """At least one batch file with rows. Empty harvest => endpoint/format broke."""
    assert _batch_asset_ids(), "no arxiv-papers-* raw batch files were written"
    rows = _load_all_papers()
    # A single bounded run harvests at least the first window (2005-09-17 alone
    # is ~94k records); be conservative against partial first windows.
    assert len(rows) >= 1000, f"only {len(rows)} records harvested; expected >= 1000"


def test_records_have_core_fields():
    """Non-deleted records must carry an id and a datestamp, and the vast
    majority must have a title — the metadataPrefix=arXiv contract. A format
    switch (e.g. silently falling back to oai_dc) would break these keys."""
    rows = _load_all_papers()
    live = [r for r in rows if not r.get("deleted")]
    assert live, "every record is flagged deleted — harvest is degenerate"

    missing_id = [r for r in live if not r.get("arxiv_id")]
    assert not missing_id, f"{len(missing_id)} live records missing arxiv_id"
    missing_ds = [r for r in live if not r.get("datestamp")]
    assert not missing_ds, f"{len(missing_ds)} live records missing datestamp"

    with_title = sum(1 for r in live if r.get("title"))
    assert with_title / len(live) >= 0.95, (
        f"only {with_title}/{len(live)} live records have a title — "
        "metadata format may have changed")


def test_arxiv_ids_unique_per_record():
    """Within a single harvest run ids should not duplicate wildly. Some overlap
    is legitimate (window overlap / partial-window re-fetch), but a near-total
    duplication signals a pagination bug re-emitting the same page."""
    rows = _load_all_papers()
    live_ids = [r["arxiv_id"] for r in rows if not r.get("deleted") and r.get("arxiv_id")]
    assert live_ids, "no live arxiv_ids found"
    distinct = len(set(live_ids))
    assert distinct / len(live_ids) >= 0.5, (
        f"only {distinct} distinct ids out of {len(live_ids)} — likely a "
        "pagination loop re-emitting pages")


def test_authors_structure():
    """authors must parse as a list of {keyname, ...} dicts on records that have
    them — verifies the nested arXiv-format parse, not a flattened string."""
    rows = _load_all_papers()
    live = [r for r in rows if not r.get("deleted")]
    with_authors = [r for r in live if r.get("authors")]
    assert with_authors, "no record carried a parsed authors list"
    sample = with_authors[0]["authors"]
    assert isinstance(sample, list) and isinstance(sample[0], dict), \
        "authors is not a list[dict] — nested parse failed"
    assert "keyname" in sample[0], "author entries missing keyname"
