"""Health-invariant tests, run post-DAG inside the connector.

Raw is written as parquet. Water collections write a single parquet per spec
(asset id == spec id). Earthquakes write one parquet batch per month, named
"usgs-events-<YYYY-MM>". We discover the batches with list_raw_files and load
each with the same loader the download fns used (load_raw_parquet).
"""

from subsets_utils import list_raw_files, load_raw_parquet

_EVENTS = "usgs-events"


def _batch_asset_ids(spec_id: str) -> list[str]:
    """Asset ids (no extension) for a spec — both the exact `<id>.parquet` and
    the batch layout `<id>-<key>.parquet`."""
    ids = []
    for pattern in (f"{spec_id}.parquet", f"{spec_id}-*.parquet"):
        for path in list_raw_files(pattern):
            name = path.split("/")[-1]
            if name.endswith(".parquet"):
                name = name[: -len(".parquet")]
            ids.append(name)
    return sorted(set(ids))


def test_every_spec_wrote_rows(spec_ids):
    """Each download spec must have written a parquet asset with rows. Empty
    payloads usually mean the endpoint changed format or paging broke."""
    for sid in spec_ids:
        assets = _batch_asset_ids(sid)
        assert assets, f"{sid}: no raw parquet written"
        total = sum(load_raw_parquet(a).num_rows for a in assets)
        assert total > 0, f"{sid}: 0 rows across {len(assets)} asset(s)"


def test_events_have_core_fields(spec_ids):
    """Earthquake rows must carry the core FDSN columns — a CSV header/format
    drift would silently strip these."""
    if _EVENTS not in spec_ids:
        return
    assets = _batch_asset_ids(_EVENTS)
    assert assets, f"{_EVENTS}: no batches"
    cols = set(load_raw_parquet(assets[0]).column_names)
    for key in ("id", "time", "mag", "latitude", "longitude"):
        assert key in cols, f"{_EVENTS} missing column '{key}': {sorted(cols)[:10]}"


def test_water_features_flattened(spec_ids):
    """Water assets must carry the geometry columns the flattener injects — a
    proxy that feature flattening ran and properties were preserved."""
    for sid in spec_ids:
        if sid == _EVENTS:
            continue
        assets = _batch_asset_ids(sid)
        assert assets, f"{sid}: no parquet asset"
        cols = set(load_raw_parquet(assets[0]).column_names)
        assert "_geometry_lon" in cols, f"{sid}: missing _geometry_lon (flatten skipped?)"
