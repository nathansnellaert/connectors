"""Post-DAG health invariants for the Bank of England download step.

Catches silent degradation that file-existence alone misses: truncated/HTML
payloads served in place of workbooks, and an IADB corpus fetch that ran but
produced no observation rows.
"""
from subsets_utils import load_raw_file, list_raw_files, load_raw_parquet

IADB_SPEC_ID = "bank-of-england-iadb-observations"


def test_static_files_are_valid_archives(spec_ids):
    """Every research XLSX / bankstats ZIP must be a non-trivial PK-zip
    container — an HTML error page or truncated download would fail the magic
    bytes / size check."""
    for sid in spec_ids:
        if sid == IADB_SPEC_ID:
            continue
        ext = "zip" if sid.endswith("bankstats-publication-tables") else "xlsx"
        content = load_raw_file(sid, extension=ext, binary=True)
        n = len(content) if content else 0
        assert n > 4096, f"{sid}: raw {ext} suspiciously small ({n} bytes)"
        assert content[:2] == b"PK", f"{sid}: not a zip/xlsx (head={content[:8]!r})"


def test_iadb_observations_have_rows(spec_ids):
    """If the IADB corpus spec ran, it must have written observation batches
    with actual rows and the expected long-format columns."""
    if IADB_SPEC_ID not in spec_ids:
        return
    files = list_raw_files(f"{IADB_SPEC_ID}-*.parquet")
    assert files, f"{IADB_SPEC_ID}: no observation batch parquet files were written"

    total_rows = 0
    for rel in files[:40]:  # sample — full corpus can be hundreds of batches
        asset = rel[:-len(".parquet")] if rel.endswith(".parquet") else rel
        # list_raw_files may return a path with a leading dir; keep only stem.
        asset = asset.rsplit("/", 1)[-1]
        table = load_raw_parquet(asset)
        assert {"series_code", "date", "value"}.issubset(set(table.column_names)), (
            f"{asset}: unexpected columns {table.column_names}"
        )
        total_rows += table.num_rows
    assert total_rows > 0, f"{IADB_SPEC_ID}: batch files present but 0 observation rows"
