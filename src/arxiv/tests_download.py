"""Post-DAG health invariants for the arXiv download node.

The corpus is harvested as monthly NDJSON batch files (`arxiv-papers-YYYY-MM`)
hanging off the single `arxiv-papers` spec, so we discover them via
list_raw_files rather than loading the spec id directly.
"""
from subsets_utils import list_raw_files, load_raw_ndjson

BATCH_GLOB = "arxiv-papers-*.ndjson.gz"


def _asset_id(path: str) -> str:
    """Strip dir prefix + '.ndjson.gz' to recover the load_raw_ndjson asset id."""
    name = path.rsplit("/", 1)[-1]
    return name[: -len(".ndjson.gz")] if name.endswith(".ndjson.gz") else name


def test_batch_files_written():
    """At least one monthly batch must exist — empty means the harvest never
    produced records (endpoint/host change, format switch, or silent auth wall)."""
    files = list_raw_files(BATCH_GLOB)
    assert files, "no arxiv-papers-*.ndjson.gz batch files were written"


def test_batches_nonempty_and_well_shaped():
    """Sampled batches load, hold rows, and carry the core arXiv fields. Catches
    truncated downloads and a parser that silently drops the metadata block."""
    files = sorted(list_raw_files(BATCH_GLOB))
    assert files, "no batch files to validate"

    # Sample head + tail so both legacy and recent months are exercised.
    sample = files[:3] + files[-3:]
    grand_total = 0
    live_seen = 0
    for path in sample:
        asset = _asset_id(path)
        rows = load_raw_ndjson(asset)
        grand_total += len(rows)
        for r in rows[:100]:
            assert r.get("arxiv_id"), f"{asset}: row missing arxiv_id"
            assert r.get("datestamp"), f"{asset}: row missing datestamp"
            if not r.get("deleted"):
                # Live records must carry identity + the nested author list.
                assert r.get("title"), f"{asset}: live record missing title"
                assert isinstance(r.get("authors"), list), f"{asset}: authors not a list"
                live_seen += 1

    assert grand_total > 0, "all sampled batches were empty"
    assert live_seen > 0, "no live (non-deleted) records found across sampled batches"
