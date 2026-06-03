"""Health invariants for the Freedom House download step.

Each spec persists one .xlsx workbook as opaque bytes. We verify the bytes are
present, non-trivially sized, and are a real OOXML zip (PK signature) that
openpyxl can open with at least one sheet — catching truncated downloads or an
error page silently saved under a 200.
"""

import io

import openpyxl

from subsets_utils import load_raw_file

# Smallest workbook we fetch is NIT at ~62KB; FOTP ~181KB; FIW files larger.
# Floor well under that to allow for annual shrinkage but catch truncation.
_MIN_BYTES = 10_000


def test_all_raw_assets_present_and_valid_xlsx(spec_ids):
    for sid in spec_ids:
        content = load_raw_file(sid, extension="xlsx", binary=True)
        assert content, f"{sid}: raw xlsx is empty"
        assert len(content) >= _MIN_BYTES, (
            f"{sid}: raw xlsx only {len(content)} bytes — likely truncated"
        )
        assert content[:2] == b"PK", (
            f"{sid}: not a zip/xlsx (head={content[:4]!r}) — likely an error page"
        )
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
        try:
            assert wb.sheetnames, f"{sid}: workbook has no sheets"
        finally:
            wb.close()
