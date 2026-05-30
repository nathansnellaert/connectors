"""Post-DAG health invariants for the CFPB download step.

complaints (CCDB, www.consumerfinance.gov) is a firehose with a per-run cap, so
a single run materializes a subset of month-batches; the test asserts at least
one well-formed, non-empty batch landed.

hmda_filers / hmda_loans live on ffiec.cfpb.gov, which is behind Akamai
bot-defense and returns 403 to datacenter / CI-runner IPs (see the node module
docstring). When that block is in force the fetch fns record a ``blocked`` marker
in state and write no asset. These tests therefore pass on EITHER outcome — a
real asset (validated) OR a recorded block — but FAIL on the silent third case
(no asset and no explanation), which would mean a genuine, unhandled regression.
"""
from subsets_utils import (
    list_raw_files,
    load_raw_parquet,
    load_state,
    raw_asset_exists,
)

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


def test_hmda_filers_present_or_blocked():
    """Filers roster is non-empty with valid LEIs, OR the host block is recorded."""
    if raw_asset_exists("cfpb-hmda-filers", "parquet"):
        table = load_raw_parquet("cfpb-hmda-filers")
        assert table.num_rows > 0, "cfpb-hmda-filers: roster parquet has 0 rows"
        for col in ("lei", "name", "count", "period"):
            assert col in table.column_names, f"cfpb-hmda-filers: missing column {col!r}"
        leis = table.column("lei").to_pylist()
        assert any(v for v in leis), "cfpb-hmda-filers: all LEIs are null/empty"
        return
    state = load_state("cfpb-hmda-filers")
    assert state.get("blocked"), (
        "cfpb-hmda-filers: no parquet AND no 'blocked' marker — expected the "
        "Akamai 403 host block on ffiec.cfpb.gov to be recorded in state"
    )


def test_hmda_loans_present_or_blocked():
    """At least one HMDA loan-year gzip-CSV batch landed, OR the block is recorded."""
    files = list_raw_files("cfpb-hmda-loans-*.csv.gz")
    if files:
        return
    state = load_state("cfpb-hmda-loans")
    assert state.get("blocked"), (
        "cfpb-hmda-loans: no csv.gz batches AND no 'blocked' marker — expected the "
        "Akamai 403 host block on ffiec.cfpb.gov to be recorded in state"
    )
