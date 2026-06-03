"""Health-invariant tests for the FDIC download step.

Run post-DAG by the connector. Catches silent degradation that file existence
alone misses — empty payloads, the ES envelope leaking through, truncated crawls.
"""

from subsets_utils import load_raw_ndjson

# Floor row counts, set well below the live totals observed during probing
# (institutions ~27.8k, failures ~4.1k, summary ~8.1k, history ~583k,
#  locations ~78k, demographics ~1.67M, financials ~1.68M, sod ~2.82M).
# A crawl returning far fewer rows than this means truncation or an API change.
_MIN_ROWS = {
    "fdic-failures": 3000,
    "fdic-summary": 5000,
    "fdic-institutions": 20000,
    "fdic-locations": 50000,
    "fdic-history": 400000,
    "fdic-demographics": 1000000,
    "fdic-financials": 1000000,
    "fdic-sod": 2000000,
}


def test_all_raw_assets_nonempty(spec_ids):
    """Every spec's raw NDJSON should hold records."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        assert len(rows) > 0, f"{sid}: raw NDJSON has 0 records"


def test_row_counts_meet_floor(spec_ids):
    """Each endpoint should return at least its expected floor of rows."""
    for sid in spec_ids:
        floor = _MIN_ROWS.get(sid)
        if floor is None:
            continue
        rows = load_raw_ndjson(sid)
        assert len(rows) >= floor, (
            f"{sid}: got {len(rows):,} records, expected >= {floor:,} — "
            f"likely a truncated crawl or upstream change"
        )


def test_records_are_unwrapped(spec_ids):
    """Records must be the real FDIC row, not the ES {data, score} wrapper.

    If unwrapping regressed, every row would be exactly {'data': {...}, 'score': N}.
    """
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        sample = rows[0]
        assert isinstance(sample, dict), f"{sid}: record is not a dict"
        assert set(sample.keys()) != {"data", "score"}, (
            f"{sid}: ES envelope leaked into raw — records not unwrapped"
        )
        # Every FDIC endpoint row carries an 'ID' field.
        assert "ID" in sample, f"{sid}: expected 'ID' field, got keys {list(sample)[:10]}"
