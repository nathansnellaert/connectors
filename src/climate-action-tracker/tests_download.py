"""Post-DAG health invariants for the Climate Action Tracker download step.

Thresholds reflect the real corpus probed from the source: ~7.9k country
emissions records, ~6.7k sector-indicator records, ~40 country ratings. We set
floors well below those so normal source churn doesn't flap, but high enough to
catch a silently-truncated or empty download.
"""
from subsets_utils import load_raw_ndjson

# Conservative floors per entity (real counts are far higher).
MIN_ROWS = {
    "climate-action-tracker-country-emissions": 5000,
    "climate-action-tracker-sector-indicators": 4000,
    "climate-action-tracker-country-ratings": 20,
}


def test_all_raw_assets_nonempty(spec_ids):
    """Every spec wrote a non-empty NDJSON asset clearing its expected floor."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        assert rows, f"{sid}: raw ndjson is empty"
        floor = MIN_ROWS.get(sid, 1)
        assert len(rows) >= floor, f"{sid}: only {len(rows)} rows (< {floor})"


def test_records_are_dicts_with_content(spec_ids):
    """Rows should be non-empty dict records, not stray scalars/strings -
    catches a format switch or a half-decoded payload."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        first = rows[0]
        assert isinstance(first, dict), f"{sid}: row 0 is {type(first).__name__}, not dict"
        assert len(first) > 0, f"{sid}: row 0 is an empty dict"


def test_emissions_have_expected_keys():
    """Country-emissions records must carry the join + value columns."""
    rows = load_raw_ndjson("climate-action-tracker-country-emissions")
    keys = set(rows[0].keys())
    for k in ("id", "region", "sector", "year", "value"):
        assert k in keys, f"country-emissions: missing key '{k}' (have {sorted(keys)})"


def test_sector_indicators_have_expected_keys():
    """Sector-indicator records must carry the join + value columns."""
    rows = load_raw_ndjson("climate-action-tracker-sector-indicators")
    keys = set(rows[0].keys())
    for k in ("id", "sector", "indicator", "year", "value"):
        assert k in keys, f"sector-indicators: missing key '{k}' (have {sorted(keys)})"
