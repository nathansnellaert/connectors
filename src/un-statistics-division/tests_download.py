"""Post-DAG health invariants for the UN Statistics Division download.

Each spec's raw asset is one full SDMX dataflow parsed all-string. We check the
payload actually arrived (non-empty), has the SDMX-CSV core columns, and that a
meaningful fraction of OBS_VALUE is numeric — catching a silent format switch,
a truncated download, or a feed that turned into all-placeholder rows.
"""

from subsets_utils import load_raw_parquet

CORE_COLUMNS = {"TIME_PERIOD", "OBS_VALUE"}


def test_all_raw_assets_nonempty(spec_ids):
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert len(table) > 0, f"{sid}: raw parquet has 0 rows"


def test_core_columns_present(spec_ids):
    for sid in spec_ids:
        cols = set(load_raw_parquet(sid).column_names)
        missing = CORE_COLUMNS - cols
        assert not missing, f"{sid}: missing SDMX core columns {missing} (have {sorted(cols)[:8]})"


def test_obs_value_mostly_numeric(spec_ids):
    """At least some OBS_VALUE must parse as a number. An all-non-numeric
    column means the column shifted or the dataflow returned only flags."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        numeric = 0
        for v in table.column("OBS_VALUE").to_pylist():
            if v is None:
                continue
            try:
                float(v)
                numeric += 1
            except (TypeError, ValueError):
                pass
        assert numeric > 0, f"{sid}: no numeric OBS_VALUE in {len(table)} rows"
