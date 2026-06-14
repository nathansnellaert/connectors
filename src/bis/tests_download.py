"""Health-invariant tests for the BIS bulk-CSV connector.

Each download asset is a gzipped NDJSON stream of observations — potentially
multi-GB (WS_LBS_D_PUB), so tests stream the first lines rather than loading the
whole asset into memory.
"""

import json

from subsets_utils import list_raw_files, raw_reader

# Streaming sample per asset — enough to prove the payload is real NDJSON of
# observations, cheap enough for the multi-GB assets.
_SAMPLE_LINES = 2000


def _sample(sid: str, limit: int = _SAMPLE_LINES) -> list[dict]:
    rows: list[dict] = []
    with raw_reader(sid, "ndjson.gz", mode="rt", compression="gzip") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
            if len(rows) >= limit:
                break
    return rows


def test_all_raw_assets_present(spec_ids):
    """Every download spec should have produced a raw NDJSON file. A missing
    file means the bulk zip vanished or the fetch crashed silently."""
    missing = [sid for sid in spec_ids if not list_raw_files(f"{sid}.ndjson.gz")]
    assert not missing, f"raw NDJSON missing for: {missing}"


def test_raw_assets_have_observations(spec_ids):
    """Sampled rows must carry a numeric OBS_VALUE and a TIME_PERIOD — guards
    against the endpoint switching format or returning the wide layout."""
    for sid in spec_ids:
        rows = _sample(sid)
        assert rows, f"{sid}: no observations in raw NDJSON"
        first = rows[0]
        assert "OBS_VALUE" in first, f"{sid}: rows lack OBS_VALUE, keys={list(first)[:8]}"
        assert "TIME_PERIOD" in first, f"{sid}: rows lack TIME_PERIOD, keys={list(first)[:8]}"
        # every sampled OBS_VALUE must be a finite number (we drop empties in fetch)
        bad = [r.get("OBS_VALUE") for r in rows if not _is_number(r.get("OBS_VALUE"))]
        assert not bad, f"{sid}: non-numeric OBS_VALUE samples: {bad[:5]}"


def _is_number(v) -> bool:
    if v is None or v == "":
        return False
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False
