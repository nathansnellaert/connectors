"""Post-DAG health invariants for the Apple chart connector.

Catches silent degradation that file-existence misses: an endpoint that started
returning empty charts, a format flip that drops item fields, or a per-country
failure that quietly halves coverage.
"""

from subsets_utils import load_raw_ndjson

REQUIRED_KEYS = {"country", "feed_type", "rank", "id", "name", "kind"}


def test_all_raw_assets_nonempty(spec_ids):
    """Every chart asset should carry rows. Empty payloads usually mean the
    endpoint switched format or the feed name went stale."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        assert len(rows) >= 10, f"{sid}: only {len(rows)} rows (expected >= 10)"


def test_rows_have_required_fields(spec_ids):
    """Item shape should be intact — required catalog/ranking fields present
    and non-null where they must be."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        sample = rows[0]
        missing = REQUIRED_KEYS - set(sample.keys())
        assert not missing, f"{sid}: row missing keys {missing}"
        # id and rank are the ranking primary key — never null.
        bad_id = sum(1 for r in rows if r.get("id") is None)
        assert bad_id == 0, f"{sid}: {bad_id} rows with null id"
        ranks = [r.get("rank") for r in rows]
        assert min(ranks) == 1, f"{sid}: ranks don't start at 1 (min={min(ranks)})"


def test_country_coverage(spec_ids):
    """A snapshot that collapsed to a single storefront signals a broken crawl
    loop rather than a genuinely region-limited feed."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        countries = {r.get("country") for r in rows}
        assert len(countries) >= 2, f"{sid}: only {countries} storefront(s) returned data"
