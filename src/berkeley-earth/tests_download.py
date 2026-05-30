"""Health invariants for the Berkeley Earth download step.

Catches silent degradation that file-existence alone misses: empty payloads,
HTML/XML error bodies served with 200, or a changed file format. The station
ZIPs are large and may be only partially downloaded on any single refresh (the
fetch fn is budget-paced and resumable), so station assertions check format +
progress, not completeness — and never read a whole multi-hundred-MB file.
"""

from subsets_utils import list_raw_files, load_raw_file, raw_reader

REGIONAL = "berkeley-earth-regional-temperature-series"
STATION = "berkeley-earth-station-observations"
_ZIP_MAGIC = b"PK\x03\x04"


def _stems(prefix: str, ext: str) -> list[str]:
    suffix = f".{ext}"
    return sorted(
        p[: -len(suffix)] for p in list_raw_files(f"{prefix}*{suffix}")
        if p.endswith(suffix)
    )


def test_regional_files_present_and_complete():
    """The regional product is a fixed set of 32 small files; all should land,
    each with the Berkeley Earth header and real numeric data rows."""
    stems = _stems(REGIONAL, "txt")
    assert len(stems) >= 32, f"{REGIONAL}: expected >=32 files, got {len(stems)}: {stems}"

    headline = f"{REGIONAL}-global-land-ocean-complete"
    text = load_raw_file(headline, extension="txt")
    assert isinstance(text, str) and len(text) > 10_000, f"{headline}: too small"
    assert "Berkeley Earth" in text, f"{headline}: missing expected header"
    data_rows = [
        ln for ln in text.splitlines()
        if ln.strip() and not ln.lstrip().startswith("%")
    ]
    assert len(data_rows) > 1000, f"{headline}: only {len(data_rows)} data rows"
    first = data_rows[0].split()
    assert len(first) >= 3 and first[0].lstrip("-").isdigit(), \
        f"{headline}: unexpected data row layout: {data_rows[0]!r}"


def test_regional_no_error_bodies():
    """No regional file should be an S3 XML error body served as text."""
    for stem in _stems(REGIONAL, "txt"):
        head = load_raw_file(stem, extension="txt")[:200]
        assert "<Error>" not in head and "AccessDenied" not in head, \
            f"{stem}: looks like an S3 error body: {head!r}"


def test_station_zips_started_and_valid():
    """Station Quality Controlled ZIPs should have begun downloading and carry
    the ZIP magic header (PK\\x03\\x04), not an XML error body. Files may be
    partial; we only read the first bytes, never the whole archive."""
    stems = _stems(STATION, "zip")
    assert stems, f"{STATION}: no station ZIP files present"

    for stem in stems:
        with raw_reader(stem, extension="zip", mode="rb") as f:
            head = f.read(64)
        assert head.startswith(_ZIP_MAGIC), \
            f"{stem}: not a ZIP (first bytes {head[:8]!r})"
