"""Health-invariant tests for the FDIC download step.

Run post-DAG by the connector. Catches silent degradation that file existence
alone misses — empty payloads, the ES envelope leaking through, truncated crawls.

MEMORY: raw assets here are huge (sod ~2.82M rows, financials ~1.68M,
demographics ~1.67M, each row carrying hundreds of fields). The prior attempt
called ``load_raw_ndjson`` — which materializes the WHOLE file as a list[dict] —
once per test per asset, spiking RSS to ~7.2GB and OOM-killing the runner. These
tests instead STREAM each asset exactly once via ``raw_reader``, parsing only the
first line and counting the rest, so peak memory is a single JSON record.
"""

import json

from subsets_utils import raw_reader

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


def _scan(sid):
    """Stream one raw NDJSON asset once. Returns (row_count, first_record).

    Memory-bounded: only the first record is retained; everything else is just
    counted line-by-line. Mirrors how the download node WROTE the asset
    (raw_writer(..., "ndjson.gz", mode="wt", compression="gzip")).
    """
    count = 0
    first = None
    with raw_reader(sid, "ndjson.gz", mode="rt", compression="gzip") as f:
        for line in f:
            if not line.strip():
                continue
            if first is None:
                first = json.loads(line)
            count += 1
    return count, first


def test_raw_assets_healthy(spec_ids):
    """Single streaming pass per asset enforces all download invariants:

    1. non-empty — empty payloads usually mean the endpoint changed format/auth.
    2. row-count floor — far fewer rows than expected => truncated crawl.
    3. records are the real FDIC row, not the ES {data, score} wrapper, and
       carry the universal 'ID' field.
    """
    for sid in spec_ids:
        count, first = _scan(sid)

        # (1) non-empty
        assert count > 0, f"{sid}: raw NDJSON has 0 records"

        # (2) floor
        floor = _MIN_ROWS.get(sid)
        if floor is not None:
            assert count >= floor, (
                f"{sid}: got {count:,} records, expected >= {floor:,} — "
                f"likely a truncated crawl or upstream change"
            )

        # (3) unwrapped real record with ID
        assert isinstance(first, dict), f"{sid}: first record is not a dict"
        assert set(first.keys()) != {"data", "score"}, (
            f"{sid}: ES envelope leaked into raw — records not unwrapped"
        )
        assert "ID" in first, (
            f"{sid}: expected 'ID' field, got keys {list(first)[:10]}"
        )
