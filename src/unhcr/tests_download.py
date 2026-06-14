"""Post-DAG health invariants for the UNHCR connector.

Catches silent degradation that file existence alone misses: an endpoint that
quietly returns an empty ZIP, a format switch that drops the measure columns,
or a parse that yields all-null dimensions.
"""

from subsets_utils import load_raw_ndjson

# `spec_ids` passed to the test harness covers every spec that ran — both the
# download nodes AND the `-transform` leaves. Only the download nodes write raw
# NDJSON; the transforms publish Delta tables. Restrict to the raw-bearing
# download ids before loading, else `load_raw_ndjson` 404s on a transform id.
def _download_ids(spec_ids):
    return [sid for sid in spec_ids if not sid.endswith("-transform")]


# Conservative floors well below observed corpus sizes (smallest, nowcasting,
# was ~170 rows; the large endpoints are 100k+). A real fetch should clear these.
_MIN_ROWS = {
    "unhcr-asylum-applications": 10000,
    "unhcr-asylum-decisions": 10000,
    "unhcr-demographics": 10000,
    "unhcr-idmc": 100,
    "unhcr-nowcasting": 20,
    "unhcr-population": 10000,
    "unhcr-solutions": 1000,
    "unhcr-unrwa": 50,
}


def test_all_raw_assets_nonempty(spec_ids):
    """Every endpoint's raw NDJSON should hold a plausible number of rows."""
    for sid in _download_ids(spec_ids):
        rows = load_raw_ndjson(sid)
        floor = _MIN_ROWS.get(sid, 1)
        assert len(rows) >= floor, f"{sid}: {len(rows)} rows < expected floor {floor}"


def test_core_dimensions_present(spec_ids):
    """Each row must carry year + origin/asylum keys, and at least some years
    must parse as integers — guards against a header/format shift."""
    for sid in _download_ids(spec_ids):
        rows = load_raw_ndjson(sid)
        sample = rows[: min(2000, len(rows))]
        for key in ("year", "country_of_origin_iso", "country_of_asylum_iso"):
            assert all(key in r for r in sample), f"{sid}: missing key {key!r}"
        years = [int(r["year"]) for r in sample if r.get("year") and str(r["year"]).isdigit()]
        assert years, f"{sid}: no parseable year values in sample"
        assert min(years) >= 1951, f"{sid}: implausible year {min(years)}"
