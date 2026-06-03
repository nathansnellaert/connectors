"""Health-invariant tests for the FiveThirtyEight download step.

Each spec stores one CSV file verbatim (bytes) via save_raw_file with a .csv
extension. These checks catch the silent-degradation failures that mere file
existence misses: empty bodies, a single truncated/header-only file, or a
payload that no longer looks like CSV (e.g. an HTML error page slipped through).

Encoding note: 538's CSVs are NOT uniformly UTF-8 — several carry accented
names in windows-1252/latin-1 (e.g. avengers.csv has byte 0xcc), so strict
utf-8 decoding raises. We store the bytes verbatim and decode with latin-1 for
these structural checks: latin-1 maps every byte 1:1 so it never raises, and it
preserves the comma/newline/ASCII structure these assertions rely on. Real
per-file re-typing is the transform step's job.
"""
from subsets_utils import load_raw_file


def _text(sid: str) -> str:
    content = load_raw_file(sid, extension="csv", binary=True)
    assert content, f"{sid}: raw CSV is empty"
    # latin-1 decodes any byte without error. A UTF-8 BOM (EF BB BF) shows up as
    # these three latin-1 chars; strip it so header checks see the real columns.
    text = content.decode("latin-1")
    if text.startswith("\xef\xbb\xbf"):
        text = text[3:]
    return text


def test_all_raw_assets_nonempty(spec_ids):
    """Every spec's raw CSV should hold a header plus at least one data row."""
    for sid in spec_ids:
        text = _text(sid)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        assert len(lines) >= 2, (
            f"{sid}: only {len(lines)} non-empty line(s) — header-only or truncated"
        )


def test_raw_assets_look_like_csv(spec_ids):
    """First line is a delimited header, and it is not a GitHub error page.

    raw.githubusercontent.com returns a tiny plain-text '404: Not Found' body on
    a missing file; raise_for_status normally catches that, but this is a cheap
    second line of defense against a malformed payload being stored as CSV."""
    for sid in spec_ids:
        text = _text(sid)
        header = text.splitlines()[0]
        assert "," in header, f"{sid}: header has no comma delimiter: {header!r}"
        assert not header.startswith("404:"), f"{sid}: stored a 404 error body"
        assert "<html" not in text[:512].lower(), f"{sid}: payload looks like HTML"
