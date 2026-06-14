"""Health invariants for the United Nations SDG download.

Catches silent degradation that file-existence checks miss: an empty series
catalog, a values firehose that wrote no batches, or batches missing the
core observation fields.
"""

from subsets_utils import load_raw_ndjson, list_raw_files


def test_series_catalog_nonempty():
    """Series/List should enumerate hundreds of series codes."""
    rows = load_raw_ndjson("united-nations-series")
    assert len(rows) > 100, f"series catalog has only {len(rows)} rows (expected >100)"
    sample = rows[0]
    for col in ("code", "description", "goal"):
        assert col in sample, f"series row missing {col!r}: {sample}"
    assert any(r.get("code") for r in rows[:50]), "series codes are all empty"


def test_values_batches_present():
    """The firehose should have written at least one per-series batch with
    real observation rows."""
    files = list_raw_files("united-nations-values-*")
    assert files, "no united-nations-values-* batch files written"

    asset = files[0].split("/")[-1]
    for suffix in (".ndjson.zst", ".ndjson.gz", ".ndjson"):
        if asset.endswith(suffix):
            asset = asset[: -len(suffix)]
            break

    rows = load_raw_ndjson(asset)
    assert len(rows) > 0, f"values batch {asset} has 0 rows"
    sample = rows[0]
    for col in ("series_code", "value", "geo_area_code", "time_period"):
        assert col in sample, f"values row missing {col!r}: {sample}"
