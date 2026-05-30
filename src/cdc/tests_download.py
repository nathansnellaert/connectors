"""Health-invariant tests for the CDC download step.

Each spec writes a gzipped NDJSON raw asset (one Socrata dataset). A dataset
that returns a permanent error (404/401/403 — restricted Public Use File)
writes no raw asset and instead leaves a TTL-bound skip marker in state, so
the coverage check tolerates a small fraction of skipped datasets but flags
specs that produced neither data nor a skip marker (a silent failure), and
flags any raw asset that is present but empty (truncated/format drift).

Loading all 734 datasets fully would be heavy, so presence is checked cheaply
via raw_asset_exists and only a sample is fully loaded to confirm non-empty,
record-shaped content.
"""

from subsets_utils import (
    raw_asset_exists,
    load_raw_ndjson,
    load_state,
)


def test_coverage_raw_or_skip(spec_ids):
    """Every spec must produce either a raw NDJSON asset or a skip marker.
    A spec with neither is a silent failure. Skipped datasets (restricted PUFs)
    are expected to be a small minority."""
    have_raw = []
    skipped = []
    missing = []
    for sid in spec_ids:
        if raw_asset_exists(sid, ext="ndjson.gz"):
            have_raw.append(sid)
        elif load_state(sid).get("skipped"):
            skipped.append(sid)
        else:
            missing.append(sid)

    assert not missing, (
        f"{len(missing)}/{len(spec_ids)} specs have neither raw asset nor skip "
        f"marker: {missing[:10]}"
    )
    # Restricted datasets are rare; if a large share went missing-as-skipped,
    # something systemic broke (auth, host change), not a few PUFs.
    assert len(have_raw) >= 0.85 * len(spec_ids), (
        f"only {len(have_raw)}/{len(spec_ids)} specs produced raw data "
        f"({len(skipped)} skipped) — expected the large majority to succeed"
    )


def test_sample_assets_nonempty_records(spec_ids):
    """A sample of raw assets must be non-empty and hold dict-shaped records.
    Catches truncated downloads and format drift (e.g. endpoint returning an
    error envelope instead of a row array)."""
    sample = [
        sid for sid in spec_ids
        if raw_asset_exists(sid, ext="ndjson.gz")
    ][:15]
    assert sample, "no raw NDJSON assets were produced at all"

    for sid in sample:
        rows = load_raw_ndjson(sid)
        assert len(rows) > 0, f"{sid}: raw NDJSON has 0 rows"
        assert isinstance(rows[0], dict), (
            f"{sid}: expected dict-shaped records, got {type(rows[0]).__name__}"
        )
