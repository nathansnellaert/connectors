"""Health invariants for the Bundesbank download step.

Runs post-DAG, in-process, through the same subsets_utils loaders the fetch
node used (save_raw_file -> load_raw_file). Catches silent degradation that
file-existence alone misses: empty payloads, truncated downloads, a surface
that quietly switched away from the Bundesbank-CSV ZIP format, error JSON saved
as data.

Coverage note: four dataless dataflows (BBBK13, BBBK20, BBDG1, BBXP1) that
exist only in the BBK metadata catalog (no observations, /rest/data 404s on
every surface) were pruned from the collect catalog, so they are NOT in the
entity union. Every spec in `spec_ids` is therefore a data-bearing flow and
must produce a valid ZIP — none are allowed to be absent.
"""
import io
import zipfile

from subsets_utils import load_raw_file, raw_asset_exists


def test_raw_assets_present_and_valid(spec_ids):
    """Every spec must hold a valid, non-empty ZIP with at least one
    data-bearing CSV member. After pruning the dataless flows there is no
    allowed-absent set — any missing asset is a real failure."""
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

    assert not missing, (
        f"{len(missing)} spec(s) missing raw assets (all should be "
        f"data-bearing after pruning): {sorted(missing)}"
    )


def test_coverage_threshold(spec_ids):
    """Sanity floor: essentially every spec must have data. Guards against a
    silent mass failure that nonetheless left the DAG green. The pruned union
    is all data-bearing, so expect full coverage."""
    present = sum(1 for sid in spec_ids if raw_asset_exists(sid, ext="zip"))
    assert present == len(spec_ids), (
        f"only {present}/{len(spec_ids)} specs have raw assets — "
        "expected full coverage of the pruned (all data-bearing) union"
    )
