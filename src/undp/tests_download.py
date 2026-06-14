"""Health invariants for the UNDP raw downloads.

These run post-DAG, in-connector, reading through subsets_utils loaders so they
behave identically locally and on CI.
"""
from subsets_utils import load_raw_parquet


def test_all_raw_assets_nonempty(spec_ids):
    """Every download spec's raw parquet must hold rows — an empty payload means
    the bulk file moved, changed format, or the melt produced nothing."""
    for sid in spec_ids:
        table = load_raw_parquet(sid)
        assert len(table) > 0, f"{sid}: raw parquet has 0 rows"


def test_composite_indices_shape():
    """The composite-indices long table should carry the expected columns, many
    countries, the HDI indicator, and a plausible year span."""
    t = load_raw_parquet("undp-composite-indices")
    cols = set(t.column_names)
    assert {"iso3", "indicator", "year", "value"} <= cols, f"missing cols: {cols}"
    # Wide CSV is ~195 countries x ~40 indicators x ~34 years; long form is large.
    assert len(t) > 100_000, f"composite-indices only {len(t)} rows (expected >100k)"

    d = t.to_pydict()
    indicators = set(d["indicator"])
    assert "hdi" in indicators, f"'hdi' indicator absent; saw {sorted(indicators)[:10]}"
    n_iso3 = len(set(d["iso3"]))
    assert n_iso3 >= 150, f"only {n_iso3} distinct iso3 (expected >=150)"
    years = [y for y in d["year"] if y is not None]
    assert min(years) <= 1990 and max(years) >= 2020, f"year span {min(years)}-{max(years)}"
    # melt should never emit a null value (we drop blanks during parse)
    assert all(v is not None for v in d["value"]), "null value leaked into composite long table"


def test_global_mpi_shape():
    """The gMPI Table 1 snapshot should have one row per country with real MPI
    values in [0, 1]."""
    t = load_raw_parquet("undp-global-mpi")
    cols = set(t.column_names)
    assert {"country", "survey", "mpi_value", "headcount_pct"} <= cols, f"missing cols: {cols}"
    assert len(t) >= 80, f"global-mpi only {len(t)} rows (expected >=80 countries)"

    d = t.to_pydict()
    mpis = [v for v in d["mpi_value"] if v is not None]
    assert len(mpis) >= 80, f"only {len(mpis)} non-null MPI values"
    assert all(0.0 <= v <= 1.0 for v in mpis), "MPI value outside [0,1]"
