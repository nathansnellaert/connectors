"""Health-invariant tests for the BIS download step.

Each spec saves the bulk *flat* CSV as a zip via save_raw_file(..., extension="zip").
A green file-exists check is not enough: a CDN hiccup or a format switch can land
a 0-byte or non-zip payload. These tests open every raw zip and confirm it holds a
real, non-trivial SDMX flat CSV (header + data rows).
"""

import io
import zipfile

from subsets_utils import load_raw_file


def _load_zip(sid):
    raw = load_raw_file(sid, extension="zip", binary=True)
    assert raw, f"{sid}: raw zip is empty"
    return zipfile.ZipFile(io.BytesIO(raw))


def test_all_raw_assets_are_valid_zips(spec_ids):
    """Every spec's raw asset is a valid zip containing exactly one CSV."""
    for sid in spec_ids:
        zf = _load_zip(sid)
        names = zf.namelist()
        assert names, f"{sid}: zip has no members"
        csvs = [n for n in names if n.lower().endswith(".csv")]
        assert csvs, f"{sid}: zip contains no CSV ({names})"


def test_csv_has_header_and_rows(spec_ids):
    """The contained CSV must be the SDMX flat form (TIME_PERIOD/OBS_VALUE
    header) and carry at least one data row."""
    for sid in spec_ids:
        zf = _load_zip(sid)
        name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        # Read just the leading bytes — these CSVs uncompress to hundreds of MB.
        with zf.open(name) as fh:
            head = fh.read(64 * 1024).decode("utf-8", errors="replace")
        lines = head.splitlines()
        assert len(lines) >= 2, f"{sid}: CSV has no data rows beyond header"
        header = lines[0]
        assert "TIME_PERIOD" in header and "OBS_VALUE" in header, (
            f"{sid}: unexpected header, not the SDMX flat form: {header[:200]}"
        )
