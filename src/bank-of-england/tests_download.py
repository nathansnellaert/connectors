"""Post-DAG health invariants for the Bank of England download step.

Catches silent degradation that file-existence alone misses: truncated XLSX/ZIP
downloads, an IADB sweep that wrote zero batches, or a CSV format flip that left
empty parquet.
"""

from subsets_utils import list_raw_files, load_raw_file, load_raw_parquet

# Bulk single-file assets and their on-disk extension. All XLSX + the Bankstats
# ZIP are ZIP containers (OOXML is a zip), so all start with the 'PK' magic.
BULK_ASSETS = {
    "bank-of-england-agents-scores": "xlsx",
    "bank-of-england-annual-boe-balance-sheet": "xlsx",
    "bank-of-england-bankstats-publication-tables": "zip",
    "bank-of-england-boc-boe-macrohistory-database": "xlsx",
    "bank-of-england-inflation-attitudes-survey-long-run": "xlsx",
    "bank-of-england-lender-of-last-resort-historical": "xlsx",
    "bank-of-england-millennium-of-macroeconomic-data-uk": "xlsx",
    "bank-of-england-nmg-household-survey": "xlsx",
    "bank-of-england-qe-related-data": "xlsx",
    "bank-of-england-weekly-boe-balance-sheet-1844-2006": "xlsx",
}

IADB_ASSET = "bank-of-england-iadb-observations"


def test_bulk_files_are_valid_archives():
    """Each XLSX/ZIP download must be a non-trivial ZIP-magic ('PK') payload.
    An HTML error page, an empty body, or a truncated download all fail here."""
    for asset, ext in BULK_ASSETS.items():
        data = load_raw_file(asset, extension=ext, binary=True)
        assert data, f"{asset}: empty raw file"
        assert len(data) > 10_000, f"{asset}: only {len(data)} bytes, looks truncated"
        assert data[:2] == b"PK", f"{asset}: not a ZIP container (got {data[:8]!r})"


def test_iadb_observations_wrote_batches():
    """The IADB firehose must have written at least one non-empty parquet batch
    with the expected long-format schema."""
    files = list_raw_files(f"{IADB_ASSET}-*.parquet")
    assert files, "IADB sweep wrote no observation batches"

    total = 0
    checked = 0
    for rel in files[:5]:
        asset = rel[: -len(".parquet")]
        table = load_raw_parquet(asset)
        assert {"series_code", "date", "value"} <= set(table.column_names), (
            f"{asset}: unexpected columns {table.column_names}"
        )
        total += table.num_rows
        checked += 1
    assert total > 0, f"IADB batches exist but hold 0 rows (checked {checked})"
