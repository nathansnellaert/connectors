"""arXiv download node — OAI-PMH ListRecords harvest of the full paper corpus.

Mechanism (precomputed by research): `oai_pmh`. We harvest
https://oaipmh.arxiv.org/oai via ListRecords with metadataPrefix=arXiv (richer
than oai_dc — split author names, categories, license). Paging is by the
self-describing resumptionToken returned on each page (page size ~1300).

Shape: record-stream firehose (shape 3). The corpus is ~2.5M+ records growing
~20k/month — far too large to re-pull in one file or one run. We segment by
*calendar month of the OAI datestamp* and write one NDJSON batch file per month
(`arxiv-papers-YYYY-MM`). The single entity-union entry `papers` maps to one
NodeSpec (`arxiv-papers`); the batch files hang off it.

Incremental / resume: state holds a monthly watermark (`YYYY-MM` = the next
month to harvest). Backfill is the first run with empty state; every run is a
bounded slice of months and advances the watermark, resuming where the last run
stopped (firehose contract). The OAI datestamp tracks record *modification*, not
submission, so a paper revised later reappears in a newer month's batch with its
new `updated` value — transform dedupes by `arxiv_id`. Each month is re-harvested
once at the start of the following month (the current month is never "closed"),
which gives natural overlap so late same-month modifications are never missed.
Every record carries a datestamp >= the repository earliest_datestamp
(2005-09-16), so harvesting all months from then forward covers the whole corpus
regardless of which month holds the legacy-migration bulk.

Raw format: NDJSON (streamed, gzip). Records are nested (author list) with many
optional fields (doi, journal-ref, msc-class, ...) — the drifty/nested case the
format rubric points at NDJSON for, not parquet.

Pacing: OAI-PMH documents no rate limit, but research recommends arXiv's 3s
defensive delay between calls; we honour it. arXiv flow-control 503s (with
Retry-After) are handled by the transient-retry decorator's backoff.
"""
import calendar
import json
import time
from datetime import date

import httpx
from lxml import etree
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, load_state, raw_writer, save_state

# --- Bump when the watermark/state contract changes (keys or cursor shape). ---
STATE_VERSION = 1

ENDPOINT = "https://oaipmh.arxiv.org/oai"
OAI_NS = "http://www.openarchives.org/OAI/2.0/"
ARX_NS = "http://arxiv.org/OAI/arXiv/"
METADATA_PREFIX = "arXiv"

# Earliest OAI datestamp the repository will accept (from Identify); a `from`
# before this errors with badArgument "start date too early". Every record has a
# datestamp >= this, so the full corpus is reachable from here forward. The first
# month's window is clamped to this exact day.
SOURCE_MIN_YEAR = 2005
SOURCE_MIN_MONTH = 9
EARLIEST_DATESTAMP = "2005-09-16"

# Defensive pacing between OAI calls (research-recommended 3s; ASCII only).
PACE_SECONDS = 3.0
# Soft per-run budget: on hit we return cleanly with the watermark advanced and
# the next refresh resumes. Expected to fire repeatedly during backfill.
MAX_FETCH_SECONDS = 1200
# Safety ceiling on pages within a single month — a real month is tens of pages;
# blowing past this means the source grew unexpectedly, so raise (don't truncate).
MAX_PAGES_PER_BUCKET = 6000
# Progress cadence (~ every few minutes at 3s/page).
LOG_EVERY_PAGES = 50

_TRANSIENT_EXC = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
    httpx.ProxyError,
)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        # arXiv OAI flow control returns 503 (with Retry-After) — transient.
        return code == 429 or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _request(params: dict) -> "etree._Element":
    """One OAI-PMH GET → parsed XML root. Retries transient transport/5xx/429."""
    resp = get(ENDPOINT, params=params, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return etree.fromstring(resp.content)


def _text(el, tag: str):
    """Stripped text of a child <tag> in the arXiv namespace, or None if absent/empty."""
    v = el.findtext(f"{{{ARX_NS}}}{tag}")
    if v is None:
        return None
    v = v.strip()
    return v or None


def _parse_record(rec) -> dict:
    """Map one OAI <record> to a flat-ish dict (authors stay nested)."""
    hdr = rec.find(f"{{{OAI_NS}}}header")
    identifier = hdr.findtext(f"{{{OAI_NS}}}identifier")
    arxiv_id = identifier.split(":")[-1] if identifier else None
    row = {
        "identifier": identifier,
        "arxiv_id": arxiv_id,
        "datestamp": hdr.findtext(f"{{{OAI_NS}}}datestamp"),
        "set_specs": [e.text for e in hdr.findall(f"{{{OAI_NS}}}setSpec")],
        "deleted": hdr.get("status") == "deleted",
    }

    arx = rec.find(f"{{{OAI_NS}}}metadata/{{{ARX_NS}}}arXiv")
    if arx is None:
        # Deleted (persistent) records carry no metadata block.
        return row

    row["id"] = _text(arx, "id")
    row["created"] = _text(arx, "created")
    row["updated"] = _text(arx, "updated")
    row["title"] = _text(arx, "title")
    row["abstract"] = _text(arx, "abstract")
    row["categories"] = _text(arx, "categories")
    row["doi"] = _text(arx, "doi")
    row["journal_ref"] = _text(arx, "journal-ref")
    row["comments"] = _text(arx, "comments")
    row["report_no"] = _text(arx, "report-no")
    row["msc_class"] = _text(arx, "msc-class")
    row["acm_class"] = _text(arx, "acm-class")
    row["license"] = _text(arx, "license")
    row["proxy"] = _text(arx, "proxy")

    authors = []
    authors_el = arx.find(f"{{{ARX_NS}}}authors")
    if authors_el is not None:
        for a in authors_el.findall(f"{{{ARX_NS}}}author"):
            affs = [aff.text for aff in a.findall(f"{{{ARX_NS}}}affiliation") if aff.text]
            authors.append(
                {
                    "keyname": a.findtext(f"{{{ARX_NS}}}keyname"),
                    "forenames": a.findtext(f"{{{ARX_NS}}}forenames"),
                    "suffix": a.findtext(f"{{{ARX_NS}}}suffix"),
                    "affiliation": affs or None,
                }
            )
    row["authors"] = authors
    return row


def _harvest_month(asset: str, frm: str, until: str) -> int:
    """Harvest one [frm, until] datestamp window, streaming to one NDJSON batch.

    Returns the number of records written. Writes nothing (creates no file) for
    an empty window. Pages via resumptionToken until exhausted.
    """
    params = {
        "verb": "ListRecords",
        "metadataPrefix": METADATA_PREFIX,
        "from": frm,
        "until": until,
    }
    n = 0
    pages = 0
    writer_cm = None
    handle = None
    try:
        while True:
            root = _request(params)

            err = root.find(f".//{{{OAI_NS}}}error")
            if err is not None:
                code = err.get("code")
                if code == "noRecordsMatch":
                    break  # empty window — normal for sparse early months
                if code == "badArgument" and "start date too early" in (err.text or ""):
                    # Window precedes the repository earliest_datestamp — nothing
                    # to harvest here. Treat as empty rather than failing the run.
                    break
                # cannotDisseminateFormat / other badArgument / badVerb are our
                # bug, not a source outage — surface loudly.
                raise RuntimeError(f"OAI error '{code}' for {asset}: {err.text}")

            records = root.findall(f".//{{{OAI_NS}}}record")
            if records and handle is None:
                writer_cm = raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip")
                handle = writer_cm.__enter__()
            for rec in records:
                handle.write(json.dumps(_parse_record(rec), separators=(",", ":")))
                handle.write("\n")
                n += 1

            pages += 1
            if pages % LOG_EVERY_PAGES == 0:
                print(f"  {asset}: {pages} pages, {n:,} records...", flush=True)
            if pages > MAX_PAGES_PER_BUCKET:
                raise RuntimeError(
                    f"{asset}: exceeded {MAX_PAGES_PER_BUCKET} pages — source grew "
                    "beyond expectations; refusing to truncate silently."
                )

            tok = root.find(f".//{{{OAI_NS}}}resumptionToken")
            if tok is None or not (tok.text and tok.text.strip()):
                break  # last page
            params = {"verb": "ListRecords", "resumptionToken": tok.text}
            time.sleep(PACE_SECONDS)
    finally:
        if writer_cm is not None:
            writer_cm.__exit__(None, None, None)
    return n


def _next_month(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)


def fetch_papers(node_id: str) -> None:
    """Harvest a bounded slice of monthly batches, advancing the watermark.

    node_id is the spec id ("arxiv-papers"); it is the state key and the prefix
    for the per-month raw assets (`arxiv-papers-YYYY-MM`).
    """
    state = load_state(node_id)
    if state.get("schema_version") not in (None, STATE_VERSION):
        print(
            f"  {node_id}: state schema_version "
            f"{state.get('schema_version')} != {STATE_VERSION}; resetting state.",
            flush=True,
        )
        state = {}

    watermark = state.get("watermark")  # "YYYY-MM" of the next month to harvest
    if watermark:
        year, month = (int(p) for p in watermark.split("-"))
    else:
        year, month = SOURCE_MIN_YEAR, SOURCE_MIN_MONTH

    today = date.today()  # frozen for this run
    current = (today.year, today.month)

    deadline = time.monotonic() + MAX_FETCH_SECONDS
    months_done = 0
    records_total = 0

    while (year, month) <= current:
        if time.monotonic() > deadline:
            print(
                f"  {node_id}: hit per-run budget at {year}-{month:02d}; "
                "resuming next refresh.",
                flush=True,
            )
            break

        frm = f"{year}-{month:02d}-01"
        if frm < EARLIEST_DATESTAMP:
            frm = EARLIEST_DATESTAMP  # repository rejects earlier `from` values
        last_day = calendar.monthrange(year, month)[1]
        until = f"{year}-{month:02d}-{last_day:02d}"
        asset = f"{node_id}-{year}-{month:02d}"

        n = _harvest_month(asset, frm, until)
        months_done += 1
        records_total += n
        print(f"  {node_id}: {year}-{month:02d} -> {n:,} records", flush=True)

        is_closed = (year, month) < current
        nxt_year, nxt_month = _next_month(year, month)
        # Write raw FIRST (done above), then advance state.
        if is_closed:
            # Past month is immutable now — advance the watermark past it.
            new_watermark = f"{nxt_year}-{nxt_month:02d}"
        else:
            # Current month stays open so the next run re-harvests it (overlap).
            new_watermark = f"{year}-{month:02d}"
        save_state(
            node_id,
            {
                "schema_version": STATE_VERSION,
                "watermark": new_watermark,
                "last_run_stats": {
                    "months": months_done,
                    "records": records_total,
                    "through": f"{year}-{month:02d}",
                    "ran_on": today.isoformat(),
                },
            },
        )
        year, month = nxt_year, nxt_month

    print(
        f"  {node_id}: run complete — {months_done} months, {records_total:,} records.",
        flush=True,
    )


DOWNLOAD_SPECS = [
    NodeSpec(id="arxiv-papers", fn=fetch_papers, kind="download"),
]
