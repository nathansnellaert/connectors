"""Post-download health invariants for the Apple top-charts connector.

These run in-process after the DAG, reading raw through the same NDJSON loader
the download node wrote with. They catch silent degradation that mere file
existence misses: empty/truncated snapshots, dropped context columns, ranks
that don't look like a real chart, or a single storefront leaking in.
"""

from collections import Counter

from subsets_utils import load_raw_ndjson

# Required context columns the download node stamps on every row.
_CONTEXT_COLS = {
    "entity_id", "country", "media", "feed_type", "segment", "rank",
    "feed_updated", "fetched_at",
}
# Core catalog fields present on every chart item regardless of media.
_ITEM_COLS = {"id", "name", "artistName", "kind", "url"}


def test_all_specs_nonempty(spec_ids):
    """Every chart family must yield rows. An empty snapshot means the endpoint
    switched format, the feed-type taxonomy moved, or all storefronts 404'd."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        assert len(rows) >= 50, f"{sid}: only {len(rows)} rows (expected >= 50)"


def test_context_and_item_columns(spec_ids):
    """Every row carries the stamped context columns and core item fields."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        sample = rows[0]
        missing_ctx = _CONTEXT_COLS - sample.keys()
        assert not missing_ctx, f"{sid}: row missing context cols {missing_ctx}"
        missing_item = _ITEM_COLS - sample.keys()
        assert not missing_item, f"{sid}: row missing item cols {missing_item}"


def test_multiple_storefronts(spec_ids):
    """A real snapshot spans many storefronts, not just the us fallback. If only
    one country shows up, the per-country loop is silently failing."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        countries = {r["country"] for r in rows}
        assert len(countries) >= 5, (
            f"{sid}: only {len(countries)} storefront(s) present: {sorted(countries)}"
        )


def test_ranks_are_sane(spec_ids):
    """Rank starts at 1 within each (country, feed_type) chart and the ids are
    distinct per chart — a degenerate response would repeat rank 1 or one id."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        per_chart = Counter()
        for r in rows:
            per_chart[(r["country"], r["feed_type"])] += 1
            assert r["rank"] >= 1, f"{sid}: non-positive rank {r['rank']}"
        # At least one chart should have a real depth of rankings.
        assert max(per_chart.values()) >= 10, (
            f"{sid}: deepest chart has only {max(per_chart.values())} entries"
        )
        ids = [r["id"] for r in rows]
        assert all(ids), f"{sid}: some rows have empty/null id"
