"""Health-invariant tests for BJS downloads.

Each spec streams one gzip-NDJSON asset of Socrata records. Several resources
are multi-million rows (population denominators ~6.3M), so these tests stream
the file line-by-line via `raw_reader` instead of loading the whole thing into
memory — counting lines and inspecting only the first record.

Floors are generous lower bounds set well below the row counts probed on
2026-05-30 (see nodes/bjs.py docstring), so a real truncation/shrink trips the
test while normal annual revisions do not.
"""

import json

from subsets_utils import raw_reader

# spec id -> generous minimum row count (~60% of observed, rounded down).
MIN_ROWS = {
    "bjs-gcuy-rt5g": 40000,     # observed 68,852  (NCVS personal victimization)
    "bjs-gkck-euys": 150000,    # observed 247,583 (NCVS household victimization)
    "bjs-iv7i-eah6": 4000,      # observed 6,978   (NIBRS property incidents)
    "bjs-kj7p-vx4s": 4000,      # observed 6,768   (NIBRS property offenses)
    "bjs-ms42-n765": 2000000,   # observed 3,725,792 (NIBRS victimization counts)
    "bjs-r32q-bdaw": 5000,      # observed 9,448   (NIBRS violent incidents)
    "bjs-r4j4-fdwx": 4000000,   # observed 6,338,824 (NCVS personal population)
    "bjs-uy37-xgmh": 400000,    # observed 722,471 (NIBRS victimization rates)
    "bjs-x3sz-eb6y": 6000,      # observed 10,828  (NIBRS violent offenses)
    "bjs-ya4e-n9zp": 3000000,   # observed 4,517,399 (NCVS household population)
}


def _count_and_first(sid):
    """Stream the gzip-NDJSON asset; return (row_count, first_record_or_None)."""
    count = 0
    first = None
    with raw_reader(sid, "ndjson.gz", mode="rt", compression="gzip") as fh:
        for line in fh:
            if not line.strip():
                continue
            if count == 0:
                first = json.loads(line)
            count += 1
    return count, first


def test_all_raw_assets_nonempty(spec_ids):
    """Every spec's NDJSON asset must hold rows — empty means the endpoint
    switched format or pagination broke silently."""
    for sid in spec_ids:
        count, _ = _count_and_first(sid)
        assert count > 0, f"{sid}: raw ndjson has 0 rows"


def test_row_floors(spec_ids):
    """Each resource clears its generous minimum row count (catches truncation)."""
    for sid in spec_ids:
        floor = MIN_ROWS.get(sid, 1)
        count, _ = _count_and_first(sid)
        assert count >= floor, (
            f"{sid}: {count} rows < expected floor {floor} — possible truncation"
        )


def test_rows_are_dicts(spec_ids):
    """Socrata returns record objects; verify the all-string shape held."""
    for sid in spec_ids:
        _, first = _count_and_first(sid)
        assert isinstance(first, dict), (
            f"{sid}: row 0 is {type(first).__name__}, not dict"
        )
        assert len(first) > 0, f"{sid}: row 0 has no fields"
