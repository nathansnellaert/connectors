"""Health-invariant tests for the ABS download step.

Raw assets are per-dataflow SDMX-CSV files written via `save_raw_file`
(extension "csv"). We load them back with `load_raw_file` and assert the
download actually produced usable CSV rather than an empty/truncated body or
a format switch. Entities legitimately retired at the source write a TTL-bound
skip marker and no raw file, so we tolerate a small fraction of missing assets
but require the overwhelming majority to be present and non-trivial.
"""
from subsets_utils import load_raw_file, load_state

# SDMX-CSV header observed across ABS dataflows; every dataflow CSV starts with
# the DATAFLOW key column and carries OBS_VALUE + TIME_PERIOD.
_REQUIRED_HEADER_TOKENS = ("DATAFLOW", "TIME_PERIOD", "OBS_VALUE")


def _load_csv(sid):
    try:
        return load_raw_file(sid, extension="csv", binary=False)
    except FileNotFoundError:
        return None


def test_most_assets_present_and_nonempty(spec_ids):
    """The vast majority of dataflows must download to a CSV with >1 line
    (header + at least one observation). A handful may be skipped (retired
    dataflows write a skip marker instead of raw); anything more is degradation."""
    present = 0
    skipped = 0
    bad = []
    for sid in spec_ids:
        text = _load_csv(sid)
        if text is None:
            st = load_state(sid)
            if st.get("skipped"):
                skipped += 1
            else:
                bad.append(f"{sid}: no raw csv and no skip marker")
            continue
        lines = text.splitlines()
        if len(lines) < 2:
            bad.append(f"{sid}: csv has {len(lines)} line(s)")
            continue
        present += 1

    assert not bad, "assets failed download integrity: " + "; ".join(bad[:15])
    # Skips should be rare. Cap at 10% of the union before we call it degradation.
    assert skipped <= 0.10 * len(spec_ids), (
        f"{skipped}/{len(spec_ids)} dataflows skipped — endpoint or auth likely broke"
    )
    assert present >= 0.90 * len(spec_ids), (
        f"only {present}/{len(spec_ids)} dataflows produced a non-empty CSV"
    )


def test_csv_header_shape(spec_ids):
    """Sampled assets must look like SDMX-CSV (DATAFLOW/TIME_PERIOD/OBS_VALUE in
    the header), catching a silent switch to XML/JSON or an error page."""
    checked = 0
    for sid in spec_ids:
        text = _load_csv(sid)
        if text is None:
            continue
        header = text.splitlines()[0].upper()
        missing = [t for t in _REQUIRED_HEADER_TOKENS if t not in header]
        assert not missing, f"{sid}: header missing {missing}: {header[:160]!r}"
        checked += 1
        if checked >= 30:
            break
    assert checked > 0, "no raw CSV assets found to validate header shape"
