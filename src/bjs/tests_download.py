"""Health invariants for BJS raw downloads.

Catches silent degradation that file-existence alone misses: empty payloads,
truncated downloads (the Socrata $limit=1000 silent-cap trap), or the key
columns the transforms depend on going missing.
"""
from subsets_utils import load_raw_parquet

# Minimum row counts observed during probing (2026-06). Set well below the real
# counts so legitimate annual revisions don't trip them, but high enough that a
# truncated/empty download fails loudly. Probed: r4j4 ~6.3M; NCVS victimization
# tables tens-to-hundreds of thousands; NIBRS estimate tables thousands+.
_MIN_ROWS = {
    "bjs-gcuy-rt5g": 10000,
    "bjs-gkck-euys": 10000,
    "bjs-r4j4-fdwx": 1000000,
    "bjs-ya4e-n9zp": 1000000,
    "bjs-iv7i-eah6": 1000,
    "bjs-kj7p-vx4s": 1000,
    "bjs-ms42-n765": 1000,
    "bjs-r32q-bdaw": 1000,
    "bjs-uy37-xgmh": 1000,
    "bjs-x3sz-eb6y": 1000,
}

_NCVS = {"bjs-gcuy-rt5g", "bjs-gkck-euys", "bjs-r4j4-fdwx", "bjs-ya4e-n9zp"}


def test_all_raw_assets_nonempty(spec_ids):
    """Every spec's raw parquet should hold rows; empty usually means the
    endpoint switched shape or the resource id went stale (404)."""
    for sid in spec_ids:
        n = len(load_raw_parquet(sid))
        assert n > 0, f"{sid}: raw parquet has 0 rows"


def test_raw_assets_meet_min_rows(spec_ids):
    """Guard against silent truncation (the Socrata 1000-row default cap)."""
    for sid in spec_ids:
        floor = _MIN_ROWS.get(sid, 1)
        n = len(load_raw_parquet(sid))
        assert n >= floor, f"{sid}: only {n} rows, expected >= {floor} (truncated?)"


def test_expected_key_columns_present(spec_ids):
    """NCVS tables must carry `year`; NIBRS tables must carry `indicator_name` +
    `estimate` — the columns the transforms key on."""
    for sid in spec_ids:
        names = set(load_raw_parquet(sid).schema.names)
        if sid in _NCVS:
            assert "year" in names, f"{sid}: missing 'year' column"
        else:
            assert {"indicator_name", "estimate"} <= names, \
                f"{sid}: missing NIBRS estimate columns"
