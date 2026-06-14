"""Health-invariant tests for the BLS connector raw layer.

Run post-DAG by the connector itself. They catch silent degradation that a
file-existence check misses: empty payloads (auth/UA regression -> 403 with no
body), format drift (column header changed), or a survey that fetched zero rows.
"""

from subsets_utils import load_raw_parquet, load_raw_ndjson

_LABSTAT_COLS = {"series_id", "year", "period", "value", "footnote_codes"}


def _download_ids(spec_ids):
    """Keep only download asset ids — drop the SqlNodeSpec '-transform' leaves.

    The harness passes every spec that ran (downloads AND transforms) in
    ``spec_ids``. Transforms publish Delta tables, not raw assets, so loading
    them with load_raw_* would 404; these health checks only inspect raw."""
    return [s for s in spec_ids if not s.endswith("-transform")]


def test_all_raw_assets_nonempty(spec_ids):
    """Every survey's raw asset must hold rows. esbr is ndjson; the rest parquet."""
    for sid in _download_ids(spec_ids):
        if sid == "bls-esbr":
            rows = load_raw_ndjson(sid)
            assert len(rows) > 0, f"{sid}: esbr ndjson has 0 indicators"
        else:
            table = load_raw_parquet(sid)
            assert len(table) > 0, f"{sid}: raw parquet has 0 rows"


def test_labstat_schema(spec_ids):
    """LABSTAT parquet must carry the documented 5-column observation layout."""
    for sid in _download_ids(spec_ids):
        if sid == "bls-esbr":
            continue
        table = load_raw_parquet(sid)
        assert _LABSTAT_COLS.issubset(set(table.column_names)), (
            f"{sid}: missing columns, got {table.column_names}"
        )


def test_esbr_has_indicators(spec_ids):
    """esbr cards must parse into rows carrying an indicator id."""
    if "bls-esbr" not in spec_ids:
        return
    rows = load_raw_ndjson("bls-esbr")
    assert any(r.get("id") for r in rows), "bls-esbr: no rows with an 'id' field"
