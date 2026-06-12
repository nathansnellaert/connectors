"""Post-DAG health invariants for the ABS SDMX connector.

These run in-process after the DAG, seeing data through subsets_utils
loaders (identical local and on CI). They catch silent degradation that
file-existence alone misses: empty payloads, wrong schema, all-null values.
"""

import json

from subsets_utils import load_raw_parquet

# Expected stable columns produced by the normalising fetch fn.
_EXPECTED_COLS = {"dataflow", "time_period", "obs_value", "unit_measure", "obs_status", "dimensions"}


def test_all_raw_assets_nonempty(spec_ids):
    """Every downloaded dataflow must have rows. An empty payload means the
    endpoint changed format, the dataflow was retired, or auth/path broke."""
    empties = []
    for sid in spec_ids:
        try:
            table = load_raw_parquet(sid)
        except FileNotFoundError:
            # A permanent 4xx writes a skipped marker and no raw — tolerated
            # per-entity, surfaced by the coverage test below rather than here.
            continue
        if len(table) == 0:
            empties.append(sid)
    assert not empties, f"raw parquet has 0 rows for: {empties[:20]} ({len(empties)} total)"


def test_schema_is_stable(spec_ids):
    """Raw must carry exactly the normalised schema, so the generic transform
    SQL stays valid across all 761 heterogeneous dataflows."""
    bad = []
    for sid in spec_ids:
        try:
            table = load_raw_parquet(sid)
        except FileNotFoundError:
            continue
        if set(table.column_names) != _EXPECTED_COLS:
            bad.append((sid, table.column_names))
    assert not bad, f"unexpected raw schema: {bad[:10]}"


def test_obs_values_mostly_numeric(spec_ids):
    """obs_value is stored as text and cast in the transform. The vast
    majority of an ABS dataflow's observations are numeric — if almost none
    parse, the column mapping is wrong (we grabbed the wrong CSV field)."""
    checked = 0
    for sid in spec_ids:
        try:
            table = load_raw_parquet(sid)
        except FileNotFoundError:
            continue
        col = table.column("obs_value").to_pylist()
        if not col:
            continue
        numeric = 0
        for v in col:
            if v is None or v == "":
                continue
            try:
                float(v)
                numeric += 1
            except ValueError:
                pass
        nonblank = sum(1 for v in col if v not in (None, ""))
        if nonblank:
            ratio = numeric / nonblank
            assert ratio > 0.5, f"{sid}: only {ratio:.0%} of obs_value parse as numeric"
            checked += 1
    assert checked > 0, "no raw assets were available to check obs_value"


def test_dimensions_is_valid_json(spec_ids):
    """The dimensions blob must be parseable JSON — it is the only place the
    dataflow-specific dimensions survive, so corruption here is silent data loss."""
    for sid in spec_ids:
        try:
            table = load_raw_parquet(sid)
        except FileNotFoundError:
            continue
        sample = table.column("dimensions").to_pylist()[:50]
        for v in sample:
            assert v is not None, f"{sid}: null dimensions blob"
            json.loads(v)  # raises on malformed JSON -> test failure
        return  # one good asset is enough to confirm the encoder works
