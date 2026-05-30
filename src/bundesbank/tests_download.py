"""Health invariants for the Bundesbank download step.

Runs post-DAG, in-process, through the same subsets_utils loaders the fetch
node used (save_raw_file -> load_raw_file). Catches silent degradation that
file-existence alone misses: empty payloads, truncated downloads, a surface
that quietly switched away from the Bundesbank-CSV ZIP format, error JSON saved
as data.

Coverage note: four union dataflows (BBBK13, BBBK20, BBDG1, BBXP1) exist in the
BBK metadata catalog but publish no observations, so /rest/data 404s for them
and the node writes no raw asset by design. Those — and only those — are
allowed to be absent.
"""
import io
import zipfile

from subsets_utils import load_raw_file, raw_asset_exists

# Dataflows that exist in metadata but publish no data (permanent 404 on
# /rest/data). Confirmed live during authoring. These specs legitimately
# produce no raw asset.
KNOWN_DATALESS = {
    "bundesbank-bbbk13",
    "bundesbank-bbbk20",
    "bundesbank-bbdg1",
    "bundesbank-bbxp1",
}


def test_raw_assets_present_and_valid(spec_ids):
    """Every spec that should have data must hold a valid, non-empty ZIP with
    at least one data-bearing CSV member; only the known-dataless flows may be
    absent, and no *unexpected* flow may be missing."""
    missing = []
    for sid in spec_ids:
        if not raw_asset_exists(sid, ext="zip"):
            missing.append(sid)
            continue

        content = load_raw_file(sid, extension="zip", binary=True)
        assert content, f"{sid}: raw zip is empty"
        assert content[:2] == b"PK", (
            f"{sid}: raw asset is not a ZIP (head={content[:16]!r}) — "
            "format may have changed"
        )
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            bad = zf.testzip()
            assert bad is None, f"{sid}: corrupt ZIP member {bad}"
            csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            assert csvs, f"{sid}: ZIP has no .csv members ({zf.namelist()})"
            # At least one CSV must carry real bytes (header + observations),
            # not just a stub.
            assert any(zf.getinfo(n).file_size > 64 for n in csvs), (
                f"{sid}: all CSV members are empty/stub-sized"
            )

    unexpected = sorted(set(missing) - KNOWN_DATALESS)
    assert not unexpected, (
        f"{len(unexpected)} spec(s) missing raw assets beyond the known "
        f"dataless set: {unexpected}"
    )


def test_coverage_threshold(spec_ids):
    """Sanity floor: the overwhelming majority of specs must have data. Guards
    against a silent mass failure that nonetheless left the DAG green."""
    present = sum(1 for sid in spec_ids if raw_asset_exists(sid, ext="zip"))
    # 86 specs, 4 known dataless -> expect 82 present.
    assert present >= 80, (
        f"only {present}/{len(spec_ids)} specs have raw assets — "
        "expected ~82 (86 minus 4 known-dataless flows)"
    )
