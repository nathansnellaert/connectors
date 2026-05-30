"""Post-DAG health invariants for the Berkeley Earth download step.

Catches silent degradation that file-existence alone misses: truncated series,
a region dropping out, an endpoint that started returning HTML/an error page
instead of a ZIP, etc. Thresholds are deliberately loose-but-nonzero so they
only trip on real breakage, not on routine monthly growth.
"""

from subsets_utils import load_raw_parquet, load_raw_file

# Regions we expect in the combined series parquet (globe + 2 hemispheres + 7 continents).
_EXPECTED_REGIONS = {
    "global",
    "northern-hemisphere",
    "southern-hemisphere",
    "africa",
    "antarctica",
    "asia",
    "australia",
    "europe",
    "north-america",
    "south-america",
}

_STATION_VARIABLES = ("TAVG", "TMIN", "TMAX")


def test_regional_series_nonempty_and_covered():
    """The combined anomaly series must have plenty of rows and cover every
    region. The corpus spans 1750-present monthly across 13 files, so tens of
    thousands of rows is the floor."""
    table = load_raw_parquet("berkeley-earth-regional-temperature-series")
    assert table.num_rows > 20000, f"only {table.num_rows} series rows"

    regions = set(table.column("region").to_pylist())
    missing = _EXPECTED_REGIONS - regions
    assert not missing, f"regions missing from series: {sorted(missing)}"

    # Year column should be plausible (Berkeley Earth starts in the 18th/19th c.).
    years = table.column("year").to_pylist()
    assert min(years) <= 1850, f"earliest year {min(years)} unexpectedly late"
    assert max(years) >= 2018, f"latest year {max(years)} unexpectedly old (truncated?)"

    # At least some monthly anomalies must be present (not an all-null column).
    anoms = table.column("monthly_anomaly").to_pylist()
    assert any(v is not None for v in anoms), "monthly_anomaly is entirely null"

    # The land+ocean combined product carries both sea-ice methods.
    methods = set(table.column("sea_ice_method").to_pylist())
    assert {"air", "water"} <= methods, f"land+ocean sea-ice sections missing: {methods}"


def test_station_archives_present_and_valid():
    """Each Quality Controlled station archive must download intact: a real ZIP
    (PK magic) of substantial size. These are 240-300 MB in practice; guard well
    below that so a truncated/partial download trips but normal variation doesn't."""
    for variable in _STATION_VARIABLES:
        content = load_raw_file(
            f"berkeley-earth-station-observations-{variable}",
            extension="zip",
            binary=True,
        )
        assert content[:4] == b"PK\x03\x04", f"{variable}: not a ZIP (got {content[:4]!r})"
        assert len(content) > 50_000_000, f"{variable}: archive only {len(content)} bytes (truncated?)"
