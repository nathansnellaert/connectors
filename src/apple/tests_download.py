"""Post-DAG health invariants for the Apple top-charts download.

These run in-process after the DAG, reading raw through subsets_utils loaders so
they behave identically locally and on CI. They catch silent degradation that
file-existence alone misses: empty snapshots, a single-storefront crawl (the
rate limiter or a 503 storm having killed every other country), or the response
shape changing so the ranking/id columns go null.
"""

from subsets_utils import load_raw_ndjson

# Apple charts are global; even niche media publish for dozens of storefronts.
# A healthy snapshot spans many countries. Set the floor well below the ~40
# storefronts we crawl so a few legitimately-empty markets don't trip it.
_MIN_COUNTRIES = 10
_REQUIRED = ("id", "name", "country", "chart", "rank", "media", "type")


def test_all_specs_nonempty(spec_ids):
    """Every chart entity must produce rows. Zero rows means the endpoint
    switched shape or every storefront 404'd."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        assert len(rows) > 0, f"{sid}: raw ndjson has 0 rows"


def test_required_fields_present_and_populated(spec_ids):
    """id/name/rank must be non-null on the vast majority of rows — null floods
    mean the JSON field names drifted."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        n = len(rows)
        for field in _REQUIRED:
            missing = sum(1 for r in rows if r.get(field) in (None, ""))
            assert missing <= 0.02 * n, (
                f"{sid}: {missing}/{n} rows missing '{field}' "
                f"(>2% — likely a response-shape change)"
            )


def test_chart_breadth(spec_ids):
    """A snapshot should span many storefronts; collapse to a handful of
    countries signals the crawl was throttled/aborted partway."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        countries = {r.get("country") for r in rows}
        assert len(countries) >= _MIN_COUNTRIES, (
            f"{sid}: only {len(countries)} storefronts present "
            f"(<{_MIN_COUNTRIES}) — crawl likely aborted"
        )


def test_ranks_are_positive_ints(spec_ids):
    """Rank is the chart position; it must be a positive integer."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        bad = [r.get("rank") for r in rows
               if not isinstance(r.get("rank"), int) or r.get("rank") < 1]
        assert not bad, f"{sid}: {len(bad)} rows with non-positive/invalid rank, e.g. {bad[:5]}"
