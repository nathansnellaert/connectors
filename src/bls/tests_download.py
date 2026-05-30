"""Health-invariant tests for the BLS LABSTAT bulk download.

Each download spec fans out into many raw files (one per file in the survey
directory), all sharing the f"{spec_id}-" prefix. These tests catch silent
degradation that file existence alone misses: a survey that wrote (almost) no
files (directory-listing parse or auth/User-Agent handshake broke), a series
catalog that vanished or came back as a 403/HTML error page, or a standard
survey whose observation partitions stopped downloading.

Design notes:
- Tolerant of per-survey layout differences. Most surveys ship xx.series +
  xx.data.N.* (some only xx.data.1.AllData, no .0.Current; la/ splits into 80+
  partitions). A few are structurally odd - esbr/ holds only seasonal-factor
  *.sf files and .gif charts with NO series/data files - so the content checks
  apply only to surveys that actually have those files, never failing esbr.
- Cheap by construction. We only ever load the (text) *_series catalog header,
  never the multi-hundred-MB *_data_* partitions, which are validated by
  presence only.
"""

from subsets_utils import list_raw_files, load_raw_file


def _assets(sid):
    return list_raw_files(prefix=f"{sid}-", extension="txt")


def _series_assets(assets):
    return [a for a in assets if a.endswith("_series")]


def _data_assets(assets):
    return [a for a in assets if "_data_" in a]


def test_every_survey_wrote_files(spec_ids):
    """Each spec must have written several raw files. Zero/one means the
    directory-listing parse or the auth/User-Agent handshake silently broke."""
    for sid in spec_ids:
        n = len(_assets(sid))
        assert n >= 3, f"{sid}: only {n} raw files written (expected a full survey directory)"


def test_series_catalogs_valid(spec_ids):
    """For every survey that ships an xx.series catalog, it must be non-empty,
    not an HTML error page, and carry the LABSTAT series_id header near the top.
    Empty / HTML payloads usually mean a 403 or error page slipping through."""
    checked = 0
    for sid in spec_ids:
        for asset in _series_assets(_assets(sid)):
            text = load_raw_file(asset, extension="txt")
            assert text and len(text) > 50, (
                f"{asset}: series catalog empty or implausibly small "
                f"({len(text) if text else 0} chars)"
            )
            head = text[:2000].lstrip()
            assert not head.lower().startswith(("<!doctype", "<html")), (
                f"{asset}: series catalog looks like an HTML error page: {head[:120]!r}"
            )
            assert "series_id" in head.lower(), (
                f"{asset}: series catalog has no 'series_id' header: {head[:120]!r}"
            )
            checked += 1
    assert checked > 0, "no survey produced a *_series catalog - something is systemically wrong"


def test_standard_surveys_have_data_partitions(spec_ids):
    """A survey that ships a series catalog should also ship at least one
    observation partition - they come as a pair in standard LABSTAT surveys.
    A series-but-no-data survey means the data files stopped downloading.
    (Surveys with no series catalog, e.g. esbr, are skipped by construction.)"""
    for sid in spec_ids:
        assets = _assets(sid)
        if _series_assets(assets):
            assert _data_assets(assets), (
                f"{sid}: has a *_series catalog but no *_data_* partition - "
                f"observation files appear to be missing"
            )
