"""Health invariants for the FIFA download step.

Raw is written as one NDJSON batch per competition under asset ids
``fifa-<entity>-<IdCompetition>``. These tests load through the same
subsets_utils loader the download node used and check the batches actually
carry well-formed records — empty/truncated payloads usually mean the v3 API
changed shape or the cursor stopped advancing.
"""

from subsets_utils import list_raw_files, load_raw_ndjson

_EXTS = (".ndjson.zst", ".ndjson.gz", ".ndjson")


def _batch_asset_ids(entity: str) -> list[str]:
    """Asset ids of every per-competition batch written for an entity."""
    ids = []
    for path in list_raw_files(f"fifa-{entity}-*.ndjson*"):
        name = path.split("/")[-1]
        for ext in _EXTS:
            if name.endswith(ext):
                ids.append(name[: -len(ext)])
                break
    return ids


def test_matches_batches_present_and_nonempty():
    """At least a meaningful slice of competitions produced match batches, and
    they hold rows. (522 competitions enumerate; matches normally finish in one
    run, but even a budget-capped partial run covers many.)"""
    ids = _batch_asset_ids("matches")
    assert len(ids) >= 20, f"expected many match batches, got {len(ids)}"

    total = 0
    for asset in ids[:50]:
        total += len(load_raw_ndjson(asset))
    assert total > 0, "match batches are all empty"


def test_matches_records_have_core_keys():
    """Each match row carries the ids transform needs to key/join on."""
    ids = _batch_asset_ids("matches")
    assert ids, "no match batches written"
    rows = load_raw_ndjson(ids[0])
    assert rows, f"{ids[0]} has no rows"
    row = rows[0]
    for key in ("IdMatch", "IdCompetition", "IdSeason", "IdStage", "Date"):
        assert key in row, f"match row missing {key}: {sorted(row)[:15]}"


def test_standings_batches_present():
    """Standings are sparse (only group/league stages have them), but across the
    competitions crawled at least one real standing batch must exist with the
    per-team columns transform expects."""
    ids = _batch_asset_ids("standings")
    assert ids, "no standing batches written"
    rows = load_raw_ndjson(ids[0])
    assert rows, f"{ids[0]} has no rows"
    row = rows[0]
    for key in ("IdCompetition", "IdSeason", "IdStage", "IdTeam", "Points"):
        assert key in row, f"standing row missing {key}: {sorted(row)[:15]}"
