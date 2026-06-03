"""Post-DAG health invariants for the Federal Reserve DDP download step.

Each spec writes one release ZIP (SDMX-ML) via save_raw_file(..., extension="zip").
These tests catch silent degradation that file-existence alone misses: HTML error
pages saved as ".zip", truncated downloads, or ZIPs missing the actual data XML.
"""

import io
import zipfile

from subsets_utils import load_raw_file


def test_all_raw_assets_are_valid_zips(spec_ids):
    """Every spec's raw asset must be a non-trivial, openable ZIP containing the
    release's SDMX data XML. A skip marker (permanent 4xx) leaves no file, so we
    tolerate a missing asset but never a corrupt one."""
    checked = 0
    for sid in spec_ids:
        try:
            content = load_raw_file(sid, extension="zip", binary=True)
        except FileNotFoundError:
            # Release was skipped this run (TTL-bound 4xx marker) — acceptable.
            continue

        assert content[:2] == b"PK", f"{sid}: raw asset is not a ZIP (magic={content[:4]!r})"
        assert len(content) > 1000, f"{sid}: ZIP is suspiciously small ({len(content)} bytes)"

        zf = zipfile.ZipFile(io.BytesIO(content))
        names = zf.namelist()
        data_xml = [n for n in names if n.endswith("_data.xml")]
        assert data_xml, f"{sid}: ZIP has no *_data.xml (members={names})"

        # The data XML should hold actual SDMX content, not an empty stub.
        raw_xml = zf.read(data_xml[0])
        assert len(raw_xml) > 500, f"{sid}: {data_xml[0]} is only {len(raw_xml)} bytes"
        assert b"MessageGroup" in raw_xml[:2000], f"{sid}: {data_xml[0]} is not SDMX-ML"
        checked += 1

    assert checked > 0, "no raw ZIP assets were produced by any spec"
