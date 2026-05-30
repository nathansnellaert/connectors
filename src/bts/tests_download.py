"""Health invariants for the BTS download step.

Raw assets are gzipped NDJSON, one JSON object per line. These tests stream
each file (never load it whole) so they stay memory-bounded even against the
~26M-row dataset, and catch the silent-degradation failure modes that mere
file existence misses: empty payloads, truncated/garbled gzip, and an
endpoint that quietly switched to returning HTML or an error envelope instead
of records.
"""
import json

from subsets_utils import raw_reader

# Four-fours whose full row count is small and known from live probing, so we
# can stream-count them cheaply and confirm the whole table came through (not
# just the default first 1000-row page).
KNOWN_COUNTS = {
    "bts-bu82-4pwz": 13,
    "bts-33xp-y9fx": 36,
    "bts-crem-w557": 953,
}


def _first_record(sid):
    with raw_reader(sid, "ndjson.gz", mode="rt", compression="gzip") as f:
        for line in f:
            line = line.strip()
            if line:
                return json.loads(line)
    return None


def test_every_asset_has_a_parseable_record(spec_ids):
    """Each spec's NDJSON must hold at least one JSON object with fields.

    Empty files or HTML/error bodies (endpoint switched format, throttled
    out) surface here rather than silently shipping a 0-row dataset.
    """
    for sid in spec_ids:
        rec = _first_record(sid)
        assert rec is not None, f"{sid}: NDJSON is empty (no records)"
        assert isinstance(rec, dict), f"{sid}: first record is {type(rec).__name__}, expected object"
        assert len(rec) > 0, f"{sid}: first record has no fields"


def test_known_small_datasets_are_complete(spec_ids):
    """Small datasets with a known size must arrive complete — guards against
    pagination stopping at the default 1000-row first page."""
    for sid, expected in KNOWN_COUNTS.items():
        if sid not in spec_ids:
            continue
        n = 0
        with raw_reader(sid, "ndjson.gz", mode="rt", compression="gzip") as f:
            for line in f:
                if line.strip():
                    n += 1
        assert n == expected, f"{sid}: got {n} rows, expected {expected}"
