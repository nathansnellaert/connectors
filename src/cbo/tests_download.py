"""Health-invariant tests for the CBO download step.

Catch silent degradation that file-existence alone misses: empty payloads,
truncated downloads, or a CSV endpoint that quietly started returning HTML
(e.g. a GitHub raw 404 page that slipped past the status check).
"""

from subsets_utils import load_raw_file


def test_all_raw_assets_nonempty_csv(spec_ids):
    """Every spec's raw CSV must hold a header plus at least one data row, and
    the first line must look like CSV (comma-delimited, not an HTML/404 page)."""
    for sid in spec_ids:
        text = load_raw_file(sid, extension="csv")
        assert isinstance(text, str), f"{sid}: raw file did not decode as UTF-8 text"
        lines = [ln for ln in text.splitlines() if ln.strip()]
        assert len(lines) >= 2, f"{sid}: expected header + >=1 data row, got {len(lines)} non-empty lines"

        header = lines[0]
        assert "," in header, f"{sid}: header has no comma, not a CSV: {header[:120]!r}"
        # GitHub's 404 body is the literal "404: Not Found" — guard against it.
        assert not header.lower().startswith("404"), f"{sid}: looks like a 404 page: {header[:120]!r}"
        assert "<html" not in text[:200].lower(), f"{sid}: looks like an HTML page, not CSV"

        # Every data row should have the same column count as the header.
        ncols = header.count(",") + 1
        first_row_cols = lines[1].count(",") + 1
        assert first_row_cols == ncols, (
            f"{sid}: first data row has {first_row_cols} cols, header has {ncols}"
        )
