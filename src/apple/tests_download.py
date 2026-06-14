"""Health invariants for the Apple top-chart raw assets.

These run post-DAG, in-connector. They catch silent degradation that mere file
existence misses: empty snapshots, charts that lost their ranking column, a feed
that switched to a single storefront, etc.
"""
from subsets_utils import load_raw_parquet


def test_all_raw_assets_nonempty(spec_ids):
    """Every chart snapshot should hold rows. An empty payload means the endpoint
    changed shape or every storefront 404'd."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert len(table) > 0, f"{sid}: raw parquet has 0 rows"


def test_chart_shape(spec_ids):
    """Ranking and identity columns must be populated — these are the spine of
    a chart snapshot."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        cols = set(table.column_names)
        for required in ("id", "rank", "country", "feed_type", "name"):
            assert required in cols, f"{sid}: missing column {required!r}"

        ids = table.column("id").to_pylist()
        assert all(v is not None for v in ids), f"{sid}: null id values present"

        ranks = [r for r in table.column("rank").to_pylist() if r is not None]
        assert ranks, f"{sid}: no rank values"
        assert min(ranks) == 1, f"{sid}: ranks should start at 1, got min={min(ranks)}"


def test_multiple_storefronts(spec_ids):
    """A live chart fetch fans out over many storefronts; collapsing to one
    country signals widespread 404/throttle failure."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        countries = set(table.column("country").to_pylist())
        assert len(countries) >= 3, (
            f"{sid}: only {len(countries)} storefront(s) returned data "
            f"({sorted(countries)}) — expected several"
        )
