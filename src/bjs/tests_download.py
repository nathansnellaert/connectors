"""Health-invariant tests for BJS downloads.

Each spec writes one NDJSON asset of Socrata records. Catch silent
degradation: empty payloads, format drift (non-dict rows), and the all-string
Socrata shape collapsing to something unexpected.
"""

from subsets_utils import load_raw_ndjson

# Per-resource floors — generous lower bounds well below observed row counts so
# a real shrink/truncation trips the test without flagging normal annual revisions.
MIN_ROWS = {
    "bjs-gcuy-rt5g": 10000,   # NCVS personal victimization
    "bjs-gkck-euys": 10000,   # NCVS household victimization
    "bjs-r4j4-fdwx": 10000,   # NCVS personal population
    "bjs-ya4e-n9zp": 10000,   # NCVS household population
    "bjs-iv7i-eah6": 500,     # NIBRS property incidents
    "bjs-kj7p-vx4s": 500,     # NIBRS property offenses
    "bjs-ms42-n765": 500,     # NIBRS victimization counts
    "bjs-r32q-bdaw": 500,     # NIBRS violent incidents
    "bjs-uy37-xgmh": 500,     # NIBRS victimization rates
    "bjs-x3sz-eb6y": 500,     # NIBRS violent offenses
}


def test_all_raw_assets_nonempty(spec_ids):
    """Every spec's NDJSON asset must hold rows."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        assert len(rows) > 0, f"{sid}: raw ndjson has 0 rows"


def test_row_floors(spec_ids):
    """Each resource clears a generous minimum row count."""
    for sid in spec_ids:
        floor = MIN_ROWS.get(sid, 1)
        rows = load_raw_ndjson(sid)
        assert len(rows) >= floor, (
            f"{sid}: {len(rows)} rows < expected floor {floor} — possible truncation"
        )


def test_rows_are_dicts(spec_ids):
    """Socrata returns an array of record objects; verify the shape held."""
    for sid in spec_ids:
        rows = load_raw_ndjson(sid)
        first = rows[0]
        assert isinstance(first, dict), f"{sid}: row 0 is {type(first).__name__}, not dict"
        assert len(first) > 0, f"{sid}: row 0 has no fields"
