"""Post-DAG health invariants for the ABS SDMX connector.

Raw assets are SDMX-CSV documents saved verbatim (one per dataflow). These
tests catch silent degradation that file-existence alone misses: missing
downloads en masse, empty/truncated payloads, or a format switch away from
SDMX-CSV (which would lose the DATAFLOW/OBS_VALUE columns the transform needs).
"""
from subsets_utils import list_raw_files, load_raw_file


def test_most_raw_assets_present(spec_ids):
    """Every union dataflow exists in the ABS catalog, so the overwhelming
    majority must produce a raw .csv. A handful may legitimately be skipped
    (retired DSD -> permanent 4xx), but a large gap means the endpoint or
    base URL broke."""
    present = [sid for sid in spec_ids if list_raw_files(f"{sid}.csv")]
    frac = len(present) / max(1, len(spec_ids))
    assert frac >= 0.95, (
        f"only {len(present)}/{len(spec_ids)} raw csv assets present "
        f"({frac:.1%}) — expected >=95%; endpoint/base-URL likely broke"
    )


def test_present_assets_are_sdmx_csv(spec_ids):
    """Sample present assets: each must be a non-trivial SDMX-CSV with the
    DATAFLOW header and an OBS_VALUE column, plus real data rows."""
    present = [sid for sid in spec_ids if list_raw_files(f"{sid}.csv")]
    assert present, "no raw csv assets found at all"
    sample = present[:: max(1, len(present) // 25)][:25]
    for sid in sample:
        text = load_raw_file(sid, extension="csv")
        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="replace")
        lines = text.splitlines()
        assert len(lines) >= 2, f"{sid}: csv has no data rows ({len(lines)} lines)"
        header = lines[0]
        assert header.startswith("DATAFLOW,"), f"{sid}: unexpected header {header[:80]!r}"
        assert "OBS_VALUE" in header, f"{sid}: no OBS_VALUE column in {header[:120]!r}"
