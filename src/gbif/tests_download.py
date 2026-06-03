"""Post-DAG health invariants for the GBIF download step.

Each test loads raw through the same subsets_utils loader the fetch fn used to
write it, so it behaves identically locally and on CI. Thresholds are set
below the observed corpus sizes (datasets ~122k, literature ~61k, backbone
~488MB) to catch silent truncation / empty payloads / format switches without
being brittle to normal growth.
"""
import gzip

from subsets_utils import load_raw_ndjson, load_raw_file, load_raw_parquet, list_raw_files


def test_datasets_nonempty():
    rows = load_raw_ndjson("gbif-datasets")
    assert len(rows) > 50_000, f"gbif-datasets: only {len(rows)} records (expected ~122k)"
    assert "key" in rows[0], f"gbif-datasets: missing 'key' field; got {list(rows[0])[:8]}"


def test_literature_nonempty():
    rows = load_raw_ndjson("gbif-literature")
    assert len(rows) > 30_000, f"gbif-literature: only {len(rows)} records (expected ~61k)"
    assert "id" in rows[0] or "title" in rows[0], (
        f"gbif-literature: missing id/title; got {list(rows[0])[:8]}"
    )


def test_species_backbone_nonempty():
    data = load_raw_file("gbif-species", "txt.gz", binary=True)
    assert len(data) > 100_000_000, (
        f"gbif-species: backbone export only {len(data)} bytes (expected ~488MB) — truncated?"
    )
    assert data[:2] == b"\x1f\x8b", "gbif-species: not a gzip stream"
    # Decompress just the first member block to confirm it is tab-delimited rows.
    head = gzip.GzipFile(fileobj=__import__("io").BytesIO(data[:1_000_000])).read(2000)
    assert b"\t" in head, "gbif-species: decompressed head is not tab-delimited"


def test_occurrences_batches_nonempty():
    files = list_raw_files("gbif-occurrences-*.parquet")
    assert files, "gbif-occurrences: no batch parquet files written"
    asset = files[0][: -len(".parquet")]
    table = load_raw_parquet(asset)
    assert table.num_rows > 0, f"gbif-occurrences: batch {asset} has 0 rows"
    assert "gbifid" in table.schema.names, (
        f"gbif-occurrences: missing 'gbifid' column; got {table.schema.names[:8]}"
    )
