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

from subsets_utils import (
    NodeSpec,
    delete_raw_file,
    get,
    list_raw_files,
    load_state,
    raw_writer,
    save_state,
)

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
# Soft per-run budget: on hit we checkpoint (resume token saved) and return
# cleanly; the next refresh resumes. Expected to fire repeatedly during backfill.
MAX_FETCH_SECONDS = 1200
# Roll to a fresh part file once a month's open part reaches this many records.
# Keeps each NDJSON file bounded (~a few hundred MB) and memory flat. Most months
# are a single part; the 2005-09 legacy-migration month (every pre-2005 paper
# datestamped at OAI population, ~hundreds of thousands of records) spans several.
RECORDS_PER_PART = 200_000
# Safety ceiling on pages harvested for one window in one run — far above any
# real month; blowing past it means the source grew unexpectedly, so raise.
MAX_PAGES_PER_RUN = 50_000
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


def _next_month(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)


def _save_progress(node_id, year, month, token, part, stats):
    """Persist a mid-month resume point. Raw is always written before this."""
    save_state(
        node_id,
        {
            "schema_version": STATE_VERSION,
            "year": year,
            "month": month,
            "token": token,
            "part": part,
            "last_run_stats": stats,
        },
    )


def _harvest_month(node_id, year, month, token, start_part, deadline, stats):
    """Harvest one month's datestamp window into NDJSON part files.

    Pages via resumptionToken. A month is split across `-pNNN` part files once a
    part reaches RECORDS_PER_PART; on the per-run budget deadline it checkpoints
    (closes the open part, saves the resume token + next part index) and stops.

    Args:
        token: resumptionToken to resume an in-progress month, or None to start
            the [from, until] window fresh.
        start_part: index of the first part file to (re)open this call.

    Returns (completed, records): `completed` False means we stopped on the
    budget mid-month and state has already been saved for resume.
    """
    frm = f"{year}-{month:02d}-01"
    if frm < EARLIEST_DATESTAMP:
        frm = EARLIEST_DATESTAMP  # repository rejects earlier `from` values
    last_day = calendar.monthrange(year, month)[1]
    until = f"{year}-{month:02d}-{last_day:02d}"

    if token:
        params = {"verb": "ListRecords", "resumptionToken": token}
    else:
        # Fresh (non-resume) harvest of this month: clear any part files left by a
        # previous, possibly larger, harvest of the same month so no stale higher
        # part lingers if the month shrank (records modified into a later month).
        for path in list_raw_files(f"{node_id}-{year}-{month:02d}-p*.ndjson.gz"):
            name = path.rsplit("/", 1)[-1]
            delete_raw_file(name[: -len(".ndjson.gz")], "ndjson.gz")
        params = {
            "verb": "ListRecords",
            "metadataPrefix": METADATA_PREFIX,
            "from": frm,
            "until": until,
        }

    part = start_part
    records = 0
    pages = 0
    cur_cm = None
    cur_handle = None
    cur_count = 0

    def _close_part():
        nonlocal cur_cm, cur_handle, cur_count
        if cur_cm is not None:
            cur_cm.__exit__(None, None, None)
            cur_cm = None
            cur_handle = None
            cur_count = 0

    try:
        while True:
            root = _request(params)

            err = root.find(f".//{{{OAI_NS}}}error")
            if err is not None:
                code = err.get("code")
                if code == "noRecordsMatch" or (
                    code == "badArgument" and "start date too early" in (err.text or "")
                ):
                    break  # empty/exhausted window — month complete
                # cannotDisseminateFormat / other badArgument / badVerb are our
                # bug, not a source outage — surface loudly.
                raise RuntimeError(f"OAI error '{code}' for {node_id} {frm}: {err.text}")

            for rec in root.findall(f".//{{{OAI_NS}}}record"):
                if cur_handle is None:
                    asset = f"{node_id}-{year}-{month:02d}-p{part:03d}"
                    cur_cm = raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip")
                    cur_handle = cur_cm.__enter__()
                    cur_count = 0
                cur_handle.write(json.dumps(_parse_record(rec), separators=(",", ":")))
                cur_handle.write("\n")
                records += 1
                cur_count += 1

            pages += 1
            if pages % LOG_EVERY_PAGES == 0:
                print(
                    f"  {node_id} {year}-{month:02d}: {pages} pages, "
                    f"{records:,} records...",
                    flush=True,
                )
            if pages > MAX_PAGES_PER_RUN:
                raise RuntimeError(
                    f"{node_id} {year}-{month:02d}: exceeded {MAX_PAGES_PER_RUN} "
                    "pages in one run — source grew beyond expectations."
                )

            tok_el = root.find(f".//{{{OAI_NS}}}resumptionToken")
            tok = (
                tok_el.text
                if tok_el is not None and tok_el.text and tok_el.text.strip()
                else None
            )
            if tok is None:
                break  # last page — month complete

            # Roll to a new part file once the open part is full.
            if cur_count >= RECORDS_PER_PART:
                _close_part()
                part += 1
                _save_progress(node_id, year, month, tok, part, stats)

            # Budget exhausted — checkpoint and stop mid-month.
            if time.monotonic() > deadline:
                _close_part()
                next_part = part + 1 if records else part
                _save_progress(node_id, year, month, tok, next_part, stats)
                print(
                    f"  {node_id}: hit per-run budget mid {year}-{month:02d}; "
                    "resuming next refresh.",
                    flush=True,
                )
                return False, records

            params = {"verb": "ListRecords", "resumptionToken": tok}
            time.sleep(PACE_SECONDS)
    finally:
        _close_part()

    return True, records


def fetch_papers(node_id: str) -> None:
    """Harvest a bounded slice of the corpus as monthly NDJSON part files.

    node_id is the spec id ("arxiv-papers"); it is the state key and the prefix
    for the per-month part assets (`arxiv-papers-YYYY-MM-pNNN`). State carries the
    current (year, month) plus a within-month resume token + part index.
    """
    state = load_state(node_id)
    if state.get("schema_version") not in (None, STATE_VERSION):
        print(
            f"  {node_id}: state schema_version "
            f"{state.get('schema_version')} != {STATE_VERSION}; resetting state.",
            flush=True,
        )
        state = {}

    year = state.get("year")
    if year:
        month = state["month"]
        token = state.get("token")
        part = state.get("part", 0)
    else:
        year, month = SOURCE_MIN_YEAR, SOURCE_MIN_MONTH
        token, part = None, 0

    today = date.today()  # frozen for this run
    current = (today.year, today.month)

    deadline = time.monotonic() + MAX_FETCH_SECONDS
    months_done = 0
    records_total = 0

    while (year, month) <= current:
        if time.monotonic() > deadline:
            _save_progress(node_id, year, month, token, part, _stats(months_done, records_total, year, month, today))
            print(f"  {node_id}: hit per-run budget at {year}-{month:02d} boundary; "
                  "resuming next refresh.", flush=True)
            return

        stats = _stats(months_done, records_total, year, month, today)
        completed, n = _harvest_month(node_id, year, month, token, part, deadline, stats)
        records_total += n
        if not completed:
            return  # checkpoint already saved inside _harvest_month

        months_done += 1
        token, part = None, 0  # next month starts fresh
        print(f"  {node_id}: {year}-{month:02d} complete -> {n:,} records", flush=True)

        if (year, month) < current:
            # Past month is immutable now — advance to the next month.
            year, month = _next_month(year, month)
            _save_progress(node_id, year, month, None, 0,
                           _stats(months_done, records_total, year, month, today))
        else:
            # Current (open) month done — reset so the next run re-harvests it
            # (overlap that catches late same-month modifications), then stop.
            _save_progress(node_id, year, month, None, 0,
                           _stats(months_done, records_total, year, month, today))
            break

    print(
        f"  {node_id}: run complete — {months_done} months, {records_total:,} records.",
        flush=True,
    )


def _stats(months, records, year, month, today):
    return {
        "months_this_run": months,
        "records_this_run": records,
        "through": f"{year}-{month:02d}",
        "ran_on": today.isoformat(),
    }


DOWNLOAD_SPECS = [
    NodeSpec(id="arxiv-papers", fn=fetch_papers, kind="download"),
]
