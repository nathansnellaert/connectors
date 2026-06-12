"""Health invariants for the arXiv firehose download.

`papers` is harvested as a sequence of NDJSON batch assets named
"arxiv-papers-<seq>", so tests glob the batch layout rather than loading a
single asset. These catch silent degradation that file-existence misses:
empty payloads, a parser that stopped yielding the core metadata fields, or a
window that returned nothing.
"""
from subsets_utils import list_raw_files, load_raw_ndjson

REQUIRED_FIELDS = ("arxiv_id", "title", "abstract", "primary_category")


def _batch_asset_ids():
    """Asset ids for every harvested batch (strip ndjson/compression suffix)."""
    ids = []
    for rel in list_raw_files("arxiv-papers-*"):
        name = rel.rsplit("/", 1)[-1]
        for suffix in (".ndjson.zst", ".ndjson.gz", ".ndjson"):
            if name.endswith(suffix):
                ids.append(name[: -len(suffix)])
                break
    return ids


def test_batches_written():
    """At least one batch landed — an empty harvest means the OAI window
    returned nothing, the endpoint moved again, or paging broke."""
    ids = _batch_asset_ids()
    assert ids, "no arxiv-papers-* batch files were written"


def test_batches_nonempty_and_shaped():
    """Every batch holds rows, and the rows carry the core metadata fields.
    Guards against the parser silently emitting blank records (e.g. an XML
    namespace change) or truncated payloads."""
    ids = _batch_asset_ids()
    assert ids, "no arxiv-papers-* batch files were written"
    total = 0
    for asset in ids:
        rows = load_raw_ndjson(asset)
        assert rows, f"{asset}: batch has 0 rows"
        total += len(rows)
        sample = rows[0]
        for field in REQUIRED_FIELDS:
            assert field in sample, f"{asset}: row missing field {field!r}"
        assert sample["arxiv_id"], f"{asset}: first row has empty arxiv_id"
        assert isinstance(sample["authors"], list), f"{asset}: authors is not a list"
    # A single OAI page is ~1000-1300 records; a healthy run writes far more.
    assert total >= 500, f"only {total} records across all batches — suspiciously low"
