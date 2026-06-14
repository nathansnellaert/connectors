"""Post-DAG health invariants for the Berkeley Earth raw assets."""
import duckdb

from subsets_utils import load_raw_parquet, raw_parquet_localpath

_REGIONAL = "berkeley-earth-regional-temperature-series"
_STATIONS = "berkeley-earth-station-observations"


def test_regional_series_nonempty_and_shaped():
    """The regional series should cover the globe plus many regions, with the
    long-format dimension columns intact."""
    t = load_raw_parquet(_REGIONAL)
    assert t.num_rows > 5000, f"{_REGIONAL}: only {t.num_rows} rows"
    expected = {"region", "surface", "variable", "year", "month",
                "anomaly", "uncertainty"}
    assert expected <= set(t.column_names), \
        f"{_REGIONAL}: missing columns {expected - set(t.column_names)}"

    regions = set(t.column("region").to_pylist())
    assert "global" in regions, "global region missing"
    assert len(regions) >= 9, f"expected globe + hemispheres + continents, got {regions}"

    variables = set(t.column("variable").to_pylist())
    assert {"TAVG", "TMAX", "TMIN"} <= variables, f"variables present: {variables}"
    surfaces = set(t.column("surface").to_pylist())
    assert {"land", "land_and_ocean"} <= surfaces, f"surfaces present: {surfaces}"

    # Anomalies are small Celsius numbers; a wildly out-of-range value means a
    # column-shift parse bug.
    anomalies = [a for a in t.column("anomaly").to_pylist() if a is not None]
    assert anomalies, "all anomalies null"
    assert max(abs(a) for a in anomalies) < 40, "anomaly out of plausible range"


def test_station_observations_nonempty_and_shaped():
    """The station archive parse should yield millions of rows across all three
    variables. Read via a local path + DuckDB so we never load ~50M rows into
    memory."""
    with raw_parquet_localpath(_STATIONS) as path:
        con = duckdb.connect()
        rel = f"read_parquet('{path}')"

        cols = {c[0] for c in con.execute(f"SELECT * FROM {rel} LIMIT 0").description}
        expected = {"station_id", "variable", "year", "month",
                    "temperature_c", "uncertainty_c"}
        assert expected <= cols, f"{_STATIONS}: missing columns {expected - cols}"

        n = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
        assert n > 5_000_000, f"{_STATIONS}: only {n} rows — truncated download?"

        nvars = con.execute(f"SELECT count(DISTINCT variable) FROM {rel}").fetchone()[0]
        assert nvars == 3, f"{_STATIONS}: expected 3 variables, found {nvars}"

        ymin, ymax = con.execute(f"SELECT min(year), max(year) FROM {rel}").fetchone()
        assert 1700 < ymin < 1900, f"min year {ymin} implausible"
        assert ymax >= 2020, f"max year {ymax} implausible — stale or mis-parsed"

        bad_month = con.execute(
            f"SELECT count(*) FROM {rel} WHERE month < 1 OR month > 12").fetchone()[0]
        assert bad_month == 0, f"{bad_month} rows with out-of-range month"
