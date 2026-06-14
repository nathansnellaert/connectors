"""Health invariants for V-Dem raw downloads.

Catches silent degradation that file-existence alone misses: an endpoint that
switched format/version, a truncated download, or an RData object that decoded
to an empty/wrong-shaped frame.
"""
from subsets_utils import load_raw_parquet

# Conservative floors well below current real sizes (v16: vdem ~28k rows /
# ~4600 cols, vparty ~12k rows / ~384 cols). A drop below these means the
# corpus shape changed and should be looked at, not silently published.
_MIN_ROWS = {"v-dem-vdem": 20000, "v-dem-vparty": 5000}
_MIN_COLS = {"v-dem-vdem": 1000, "v-dem-vparty": 200}


def test_all_raw_assets_nonempty(spec_ids):
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert len(table) > 0, f"{sid}: raw parquet has 0 rows"


def test_raw_assets_meet_floors(spec_ids):
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        floor = _MIN_ROWS.get(sid)
        if floor is not None:
            assert len(table) >= floor, (
                f"{sid}: {len(table)} rows < expected floor {floor} — "
                "corpus may be truncated or the file format changed"
            )
        col_floor = _MIN_COLS.get(sid)
        if col_floor is not None:
            assert table.num_columns >= col_floor, (
                f"{sid}: {table.num_columns} columns < expected floor "
                f"{col_floor} — the bundled RData object may have changed"
            )


def test_key_columns_present(spec_ids):
    """Panel keys the transform filters on must exist in every raw asset."""
    for sid in spec_ids:
        names = set(load_raw_parquet(sid).schema.names)
        assert "year" in names, f"{sid}: missing 'year' column"
        if sid == "v-dem-vdem":
            assert "country_id" in names, f"{sid}: missing 'country_id'"
        if sid == "v-dem-vparty":
            assert "v2paid" in names, f"{sid}: missing 'v2paid'"
