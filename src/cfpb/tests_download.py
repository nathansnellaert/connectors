"""Post-DAG health invariants for the CFPB download step.

The two large entities are firehoses with per-run caps, so a single run only
materializes a subset of batches (some complaint-months, one HMDA loan-year).
These tests therefore assert that *at least one* batch landed and is well
formed — catching empty/truncated payloads and format drift — rather than
demanding the whole corpus.
"""
from subsets_utils import list_raw_files, load_raw_parquet

EXPECTED_COMPLAINT_COLS = 18


def test_complaints_batches_present_and_nonempty():
    """At least one complaints month-batch parquet exists, with the expected
    18-column schema and a non-zero row count."""
    files = list_raw_files("cfpb-complaints-*.parquet")
    assert files, "no cfpb-complaints-*.parquet batches were written"
    asset = files[0][: -len(".parquet")]
    table = load_raw_parquet(asset)
    assert table.num_columns == EXPECTED_COMPLAINT_COLS, (
        f"{asset}: expected {EXPECTED_COMPLAINT_COLS} columns, got {table.num_columns}"
    )
    assert "Complaint ID" in table.column_names, f"{asset}: missing 'Complaint ID' column"
    assert table.num_rows > 0, f"{asset}: complaints batch has 0 rows"


def test_hmda_filers_nonempty():
    """The filers roster is a full re-pull; it must hold rows with a valid LEI."""
    table = load_raw_parquet("cfpb-hmda-filers")
    assert table.num_rows > 0, "cfpb-hmda-filers: roster parquet has 0 rows"
    for col in ("lei", "name", "count", "period"):
        assert col in table.column_names, f"cfpb-hmda-filers: missing column {col!r}"
    leis = table.column("lei").to_pylist()
    assert any(v for v in leis), "cfpb-hmda-filers: all LEIs are null/empty"


def test_hmda_loans_batch_present():
    """At least one HMDA loan-year gzip-CSV batch landed (firehose, 1 year/run)."""
    files = list_raw_files("cfpb-hmda-loans-*.csv.gz")
    assert files, "no cfpb-hmda-loans-*.csv.gz batches were written"
