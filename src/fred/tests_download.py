"""Post-DAG health invariants for the FRED download step.

Run in-process by the connector after the DAG, through the same subsets_utils
loaders the download nodes used — so they behave identically locally and on CI.

Shapes under test:
  - fred-releases     : one NDJSON asset, full pull, must hold rows.
  - fred-series       : per-release NDJSON batches (fred-series-<id>.ndjson.gz).
  - fred-observations : per-release NDJSON batches (fred-observations-<id>.ndjson.gz).

The firehose specs are soft-capped per run, so we don't assert full coverage —
only that at least one batch landed and that batches carry rows (empty payloads
mean the endpoint shape or auth changed silently).
"""
from subsets_utils import load_raw_ndjson, list_raw_files


def test_releases_nonempty():
    """The releases catalog should hold the ~300 FRED releases."""
    rows = load_raw_ndjson("fred-releases")
    assert len(rows) > 0, "fred-releases: NDJSON has 0 rows"
    # Sanity: release records should expose an id field under one of the known names.
    sample = rows[0]
    assert any(k in sample for k in ("id", "release_id")), (
        f"fred-releases: record missing an id field; keys={list(sample)[:10]}"
    )


def test_series_batches_present_and_nonempty():
    """At least one per-release series batch landed, and batches carry rows."""
    _assert_firehose_batches("fred-series")


def test_observations_batches_present_and_nonempty():
    """At least one per-release observations batch landed, and batches carry rows."""
    _assert_firehose_batches("fred-observations")


def _assert_firehose_batches(prefix: str) -> None:
    files = list_raw_files(f"{prefix}-*.ndjson.gz")
    assert files, (
        f"{prefix}: no per-release batch files found — the firehose processed "
        f"zero releases (check route/auth) or wrote nothing."
    )
    # Spot-check the first few batches for actual content. A batch with no rows
    # means a truncated download or a wrong response envelope.
    checked = 0
    total_rows = 0
    for rel in files[:3]:
        asset = rel[: -len(".ndjson.gz")]
        rows = load_raw_ndjson(asset)
        assert rows, f"{prefix}: batch {rel} decoded to 0 rows"
        total_rows += len(rows)
        checked += 1
    assert total_rows > 0, f"{prefix}: checked {checked} batches, all empty"
