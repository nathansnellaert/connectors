"""Health invariants for the UNEP values download.

These run post-DAG, in-connector, against the raw asset through the same loader
the download node wrote it with. They catch silent degradation that mere file
existence misses: an empty/gutted payload (viewer down again), a format switch,
or coverage collapsing to a handful of indicators.
"""
from subsets_utils import load_raw_ndjson

# A 5-indicator probe already yielded ~1.7k observations; the full ~1.9k-indicator
# corpus is far larger. These floors are deliberately well below the real volume
# so they fire only on genuine breakage, not on normal source revisions.
MIN_ROWS = 5_000
MIN_INDICATORS = 100
REQUIRED_KEYS = {"indicator_id", "country_id", "year", "observation"}


def test_values_nonempty(spec_ids):
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        assert len(rows) >= MIN_ROWS, (
            f"{sid}: only {len(rows)} observations (< {MIN_ROWS}); "
            "WESR SDG-data viewer may be down or returning empty payloads"
        )


def test_values_shape(spec_ids):
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        sample = rows[0]
        missing = REQUIRED_KEYS - set(sample.keys())
        assert not missing, f"{sid}: rows missing keys {missing}; got {sorted(sample.keys())}"
        # observation must be numeric, year must be a sane integer year
        assert isinstance(sample["observation"], (int, float)), (
            f"{sid}: observation is {type(sample['observation']).__name__}, expected numeric"
        )
        assert all(1900 <= int(r["year"]) <= 2100 for r in rows[:1000]), (
            f"{sid}: years out of range in sample"
        )


def test_indicator_coverage(spec_ids):
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        n_ind = len({r["indicator_id"] for r in rows})
        assert n_ind >= MIN_INDICATORS, (
            f"{sid}: only {n_ind} distinct indicators (< {MIN_INDICATORS}); "
            "indicator enumeration or value resolution degraded"
        )
