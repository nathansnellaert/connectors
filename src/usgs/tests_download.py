"""Health-invariant tests, run post-DAG inside the connector.

Raw is written as NDJSON batches (firehose shape) — one asset per page/window,
named "<spec-id>-<batch-key>". We discover batches with list_raw_files and load
them with the same loader the download fns used (load_raw_ndjson).
"""

from subsets_utils import list_raw_files, load_raw_ndjson

_NDJSON_EXTS = (".ndjson.zst", ".ndjson.gz", ".ndjson")


def _asset_ids(spec_id: str) -> list[str]:
    ids = []
    for path in list_raw_files(f"{spec_id}-*"):
        name = path.split("/")[-1]
        for ext in _NDJSON_EXTS:
            if name.endswith(ext):
                name = name[: -len(ext)]
                break
        ids.append(name)
    return ids


def test_every_spec_wrote_rows(spec_ids):
    """Each download spec must have written at least one batch with rows.
    Empty payloads usually mean the endpoint changed format or paging broke."""
    for sid in spec_ids:
        assets = _asset_ids(sid)
        assert assets, f"{sid}: no raw NDJSON batches written"
        total = sum(len(load_raw_ndjson(a)) for a in assets)
        assert total > 0, f"{sid}: 0 rows across {len(assets)} batch(es)"


def test_events_have_core_fields(spec_ids):
    """Earthquake rows must carry the core FDSN columns — a CSV header/format
    drift would silently strip these."""
    if "usgs-events" not in spec_ids:
        return
    assets = _asset_ids("usgs-events")
    assert assets, "usgs-events: no batches"
    rows = load_raw_ndjson(assets[0])
    assert rows, "usgs-events: first batch empty"
    sample = rows[0]
    for key in ("id", "time", "mag", "latitude", "longitude"):
        assert key in sample, f"usgs-events row missing '{key}': {sorted(sample)[:10]}"


def test_water_features_flattened(spec_ids):
    """Water rows must carry the geometry columns the flattener injects — a
    proxy that feature flattening ran and properties were preserved."""
    for sid in spec_ids:
        if sid == "usgs-events":
            continue
        assets = _asset_ids(sid)
        assert assets, f"{sid}: no batches"
        rows = load_raw_ndjson(assets[0])
        assert rows, f"{sid}: first batch empty"
        assert "_geometry_lon" in rows[0], f"{sid}: missing _geometry_lon (flatten skipped?)"
