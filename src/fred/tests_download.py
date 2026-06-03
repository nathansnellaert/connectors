"""Post-DAG health invariants for the FRED download step.

Run in-process by the connector after the DAG, through the same subsets_utils
loaders the download nodes used — so they behave identically locally and on CI.

Shapes under test (all NDJSON):
  - fred-releases     : one asset, full pull (v1 /fred/releases), must hold rows
                        carrying a release id.
  - fred-series       : per-release batches fred-series-<id>.ndjson.gz (v1
                        /fred/release/series), rows carry a series id + _release_id.
  - fred-observations : per-release batches fred-observations-<id>.ndjson.gz (v2
                        /fred/v2/release/observations), rows are flattened
                        {series_id, date, value, release_id}.

The firehose specs are soft-capped per run, so we don't assert full corpus
coverage — only that at least one batch landed and batches carry well-formed rows.
Empty payloads or a missing id field usually mean the endpoint shape or auth
changed silently, which file-existence alone would miss.
"""
from subsets_utils import load_raw_ndjson, list_raw_files


def test_releases_nonempty():
    """The releases catalogue should hold the FRED releases (~300)."""
    rows = load_raw_ndjson("fred-releases")
    assert len(rows) > 0, "fred-releases: NDJSON has 0 rows"
    sample = rows[0]
    assert any(k in sample for k in ("id", "release_id")), (
        f"fred-releases: record missing an id field; keys={list(sample)[:10]}"
    )


def test_series_batches_present_and_nonempty():
    """At least one per-release series batch landed, rows carry a series id."""
    rows = _first_batch_rows("fred-series")
    sample = rows[0]
    assert any(k in sample for k in ("id", "series_id")), (
        f"fred-series: record missing a series id field; keys={list(sample)[:12]}"
    )
    assert "_release_id" in sample, (
        f"fred-series: record missing _release_id stamp; keys={list(sample)[:12]}"
    )


def test_observations_batches_present_and_nonempty():
    """At least one per-release observations batch landed, rows are flattened
    (series_id, date, value, release_id) — guards against the v2 envelope shape
    drifting or pagination silently truncating to nothing."""
    rows = _first_batch_rows("fred-observations")
    sample = rows[0]
    for field in ("series_id", "date", "value", "release_id"):
        assert field in sample, (
            f"fred-observations: row missing '{field}'; keys={list(sample)[:12]}"
        )


def _first_batch_rows(prefix: str) -> list[dict]:
    """Locate the firehose batches for `prefix`, assert at least one exists, and
    return the rows of the first non-empty batch (asserting non-emptiness)."""
    files = list_raw_files(f"{prefix}-*.ndjson.gz")
    assert files, (
        f"{prefix}: no per-release batch files found — the firehose processed "
        f"zero releases (check route/auth) or wrote nothing."
    )
    for rel in files[:5]:
        asset = rel[: -len(".ndjson.gz")]
        rows = load_raw_ndjson(asset)
        if rows:
            return rows
    raise AssertionError(
        f"{prefix}: checked the first {min(5, len(files))} batches, all decoded "
        f"to 0 rows — truncated download or wrong response envelope."
    )
