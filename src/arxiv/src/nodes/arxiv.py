"""arXiv download node — OAI-PMH metadata harvest.

The entity union has a single entity, `papers`: the full arXiv metadata
corpus (~2.6M+ records, growing ~20k/month). Research's chosen mechanism is
`oai_pmh` — harvest via ListRecords with metadataPrefix=arXiv against the
March-2025 host https://oaipmh.arxiv.org/oai, paging resumptionTokens to
exhaustion.

Incremental shape: date-filtered (crib shape (a)). OAI-PMH `from`/`until`
filter on the record *datestamp* (modification date, YYYY-MM-DD granularity).
State holds the max datestamp seen as a watermark; each refresh re-harvests
from `watermark - OVERLAP` so revised records are re-picked up. There is no
terminal flag — every run does an incremental harvest, the first one (empty
state) being the full backfill from the source's earliest datestamp.

Resumability — why this matters here
------------------------------------
The full backfill is ~2700 ListRecords pages and runs over an hour. The prior
attempt was orphaned mid-harvest (executor killed ~100 min in) and lost all
~1.2M harvested records because state was only persisted at the very end. This
node is built so a killed run resumes instead of restarting:

  * Each page is written as its own complete gzip member (open/append/close),
    so a kill between pages always leaves a valid, readable raw file.
  * After every page the resumptionToken is checkpointed to state. A killed
    run reopens the raw file in append mode and continues from that token.
  * On resume the existing raw file is scanned end-to-end; if it is corrupt
    (a kill landed inside a member flush) the harvest restarts cleanly.
  * Raw is always written before state — a crash in between costs one
    duplicated page (transform dedups by id-keyed merge), never a lost one.

Pacing
------
Research suggested a defensive 3s inter-page delay (the arXiv REST query API's
recommendation). Applied to ~2700 OAI-PMH pages that is ~135 min of pure sleep
and is what made the prior run un-completable. OAI-PMH bulk harvesting is
instead paced by the server itself: arXiv returns HTTP 503 with a Retry-After
when a harvester should back off. We therefore do no client-side delay and let
the retry decorator's exponential backoff absorb 503 flow control — this is the
canonical OAI-PMH harvesting contract.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
from lxml import etree
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from subsets_utils import (
    NodeSpec,
    configure_http,
    get,
    load_state,
    raw_reader,
    raw_writer,
    save_state,
)

OAI_ENDPOINT = "https://oaipmh.arxiv.org/oai"
METADATA_PREFIX = "arXiv"  # split authors + categories + license; richer than oai_dc

# OAI-PMH XML namespaces.
OAI_NS = "http://www.openarchives.org/OAI/2.0/"
# arXiv's native metadata format namespace (metadataPrefix=arXiv).
ARXIV_NS = "http://arxiv.org/OAI/arXiv/"

# Earliest datestamp from the chosen mechanism's Identify response.
SOURCE_MIN = "2005-09-16"

# One announcement period (~daily) plus generous revision lag — arXiv datestamps
# move whenever a paper is revised, withdrawn, or recategorised. Overlap dups are
# dedup'd downstream by transform's id-keyed merge; they are the safety net.
OVERLAP = timedelta(days=14)

# Safety cap on pages per run. Backfill is ~2700 pages at 1000 records/page;
# this is ~4x headroom and also catches a resumptionToken that loops. Hitting it
# raises — silent truncation would hide corpus growth for months.
MAX_PAGES = 12000

# State schema. v1 was the prior attempt's end-of-run-only watermark; v2 adds
# the mid-harvest `resume` checkpoint. Unknown versions reset state to empty.
STATE_SCHEMA_VERSION = 2

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
        # OAI-PMH flow control surfaces as 503; 429 + other 5xx are transient.
        return code == 429 or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(12),
    # min=10 honours arXiv's typical 503 Retry-After window; max caps the wait.
    wait=wait_exponential(multiplier=2, min=10, max=180),
    reraise=True,
)
def _fetch_page(params: dict) -> bytes:
    """One OAI-PMH request. Retries transient transport / 5xx / 429 failures."""
    resp = get(
        OAI_ENDPOINT,
        params=params,
        timeout=httpx.Timeout(connect=15.0, read=300.0, write=30.0, pool=30.0),
    )
    resp.raise_for_status()
    return resp.content


def _text(el, tag: str):
    """Stripped text of a child element in the arXiv metadata namespace."""
    v = el.findtext(f"{{{ARXIV_NS}}}{tag}")
    return v.strip() if v and v.strip() else None


def _parse_arxiv_metadata(el) -> dict:
    """Parse one <arXiv> metadata block into a flat-ish dict."""
    authors = []
    authors_el = el.find(f"{{{ARXIV_NS}}}authors")
    if authors_el is not None:
        for a in authors_el.findall(f"{{{ARXIV_NS}}}author"):
            authors.append(
                {
                    "keyname": _text(a, "keyname"),
                    "forenames": _text(a, "forenames"),
                    "suffix": _text(a, "suffix"),
                    "affiliation": _text(a, "affiliation"),
                }
            )
    categories = _text(el, "categories")
    return {
        "id": _text(el, "id"),
        "created": _text(el, "created"),
        "updated": _text(el, "updated"),
        "title": _text(el, "title"),
        "abstract": _text(el, "abstract"),
        "categories": categories,
        "primary_category": (categories or "").split(" ")[0] or None,
        "doi": _text(el, "doi"),
        "journal_ref": _text(el, "journal-ref"),
        "comments": _text(el, "comments"),
        "license": _text(el, "license"),
        "authors": authors,
    }


def _parse_page(xml_bytes: bytes):
    """Parse one ListRecords response.

    Returns (records, resumption_token, error_code, complete_list_size).
    error_code is set only when the response is an OAI-PMH <error> element;
    resumption_token is None on the final page.
    """
    root = etree.fromstring(xml_bytes)

    err = root.find(f"{{{OAI_NS}}}error")
    if err is not None:
        return [], None, (err.get("code") or "unknown"), None

    list_records = root.find(f"{{{OAI_NS}}}ListRecords")
    if list_records is None:
        # Neither ListRecords nor error — malformed; a hard failure, not a bug.
        raise RuntimeError("OAI-PMH response had neither ListRecords nor error")

    records = []
    for rec in list_records.findall(f"{{{OAI_NS}}}record"):
        header = rec.find(f"{{{OAI_NS}}}header")
        row = {
            "oai_identifier": header.findtext(f"{{{OAI_NS}}}identifier"),
            "datestamp": header.findtext(f"{{{OAI_NS}}}datestamp"),
            "status": header.get("status"),  # "deleted" for tombstoned records
            "set_specs": [
                s.text for s in header.findall(f"{{{OAI_NS}}}setSpec") if s.text
            ],
        }
        metadata = rec.find(f"{{{OAI_NS}}}metadata")
        if metadata is not None:
            arxiv_el = metadata.find(f"{{{ARXIV_NS}}}arXiv")
            if arxiv_el is not None:
                row["metadata"] = _parse_arxiv_metadata(arxiv_el)
        records.append(row)

    token_el = list_records.find(f"{{{OAI_NS}}}resumptionToken")
    token = None
    complete_size = None
    if token_el is not None:
        if token_el.text and token_el.text.strip():
            token = token_el.text.strip()
        cls = token_el.get("completeListSize")
        if cls and cls.isdigit():
            complete_size = int(cls)
    return records, token, None, complete_size


def _write_page(asset: str, records: list, *, truncate: bool) -> None:
    """Append one page of records as a self-contained gzip member.

    truncate=True opens the raw file fresh (first write of a fresh harvest);
    otherwise the member is concatenated onto the existing file. Each member
    is opened-written-closed here, so a kill between pages always leaves a
    fully valid, readable .ndjson.gz.
    """
    mode = "wt" if truncate else "at"
    with raw_writer(asset, "ndjson.gz", mode=mode, compression="gzip") as f:
        for row in records:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _scan_raw(asset: str):
    """Scan an existing raw file end-to-end.

    Returns (readable, record_count, max_datestamp). readable is False when the
    file is missing, empty, or corrupt (a kill landed inside a member flush) —
    in which case the caller restarts the harvest fresh rather than appending
    onto an unreadable file.
    """
    count = 0
    max_ds = None
    try:
        with raw_reader(asset, "ndjson.gz", mode="rt", compression="gzip") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                count += 1
                ds = row.get("datestamp")
                if ds and (max_ds is None or ds > max_ds):
                    max_ds = ds
    except Exception as e:  # noqa: BLE001 - log the class, then restart fresh
        print(
            f"[arxiv] existing raw for {asset} unreadable "
            f"({type(e).__name__}: {e}); restarting harvest fresh",
            flush=True,
        )
        return False, 0, None
    return count > 0, count, max_ds


def fetch_papers(entity_id: str) -> None:
    """Harvest the arXiv metadata corpus via OAI-PMH ListRecords.

    Fresh run: full backfill (empty state) or an incremental window
    [watermark - OVERLAP, today]. Resumed run: continues a prior, interrupted
    harvest from its checkpointed resumptionToken.
    """
    asset = f"arxiv-{entity_id.lower().replace('_', '-')}"

    # Identify the harvester to arXiv (ASCII-only header, OAI-PMH etiquette).
    configure_http(
        headers={
            "User-Agent": "subsets-arxiv-harvester/1.0 "
            "(+https://subsets.io; contact: data@subsets.io)"
        }
    )

    state = load_state(asset)
    if state.get("schema_version") not in (None, STATE_SCHEMA_VERSION):
        print(
            f"[arxiv] unknown state schema_version {state.get('schema_version')!r}; "
            "resetting state and harvesting fresh",
            flush=True,
        )
        state = {}

    prior_watermark = state.get("watermark")  # max datestamp of last completed run

    # The fresh-harvest window — used both for a clean start and for recovering
    # from an expired resumptionToken. `until` is frozen for the whole run so
    # pagination doesn't drift while crawling.
    if prior_watermark:
        wm = datetime.strptime(prior_watermark, "%Y-%m-%d").date()
        fresh_from = (wm - OVERLAP).isoformat()
    else:
        fresh_from = SOURCE_MIN
    fresh_until = datetime.now(timezone.utc).date().isoformat()

    # Decide: resume an interrupted harvest, or start a fresh one.
    resume = state.get("resume")
    token = None
    from_date = fresh_from
    until_date = fresh_until
    records_written = 0
    max_datestamp = prior_watermark
    first_write_truncates = True

    if resume and resume.get("token"):
        ok, count, scanned_max = _scan_raw(asset)
        if ok:
            token = resume["token"]
            from_date = resume.get("from", fresh_from)
            until_date = resume.get("until", fresh_until)
            records_written = count
            max_datestamp = scanned_max or prior_watermark
            first_write_truncates = False  # append onto the validated raw file
            print(
                f"[arxiv] resuming {asset}: {count} records already on disk, "
                f"window from={from_date} until={until_date}",
                flush=True,
            )
        else:
            print(f"[arxiv] resume checkpoint unusable for {asset}; fresh start", flush=True)

    if first_write_truncates:
        print(
            f"[arxiv] fresh harvest {asset}: from={from_date} until={until_date} "
            f"(prior watermark={prior_watermark})",
            flush=True,
        )

    page = 0
    complete_size = None

    while True:
        page += 1
        if page > MAX_PAGES:
            raise RuntimeError(
                f"[arxiv] exceeded MAX_PAGES={MAX_PAGES} for {asset} — corpus grew "
                "past the safety cap or a resumptionToken is looping; investigate "
                "before raising the cap."
            )

        if token is None:
            params = {
                "verb": "ListRecords",
                "metadataPrefix": METADATA_PREFIX,
                "from": from_date,
                "until": until_date,
            }
        else:
            # OAI-PMH: on resumption the request carries verb + token only.
            params = {"verb": "ListRecords", "resumptionToken": token}

        used_token = token
        xml_bytes = _fetch_page(params)
        records, next_token, error_code, page_size = _parse_page(xml_bytes)
        if page_size is not None:
            complete_size = page_size

        if error_code == "noRecordsMatch":
            # Empty window — a valid OAI-PMH result. Nothing to harvest; the
            # prior raw file (if any) is left untouched.
            print(f"[arxiv] noRecordsMatch — no records in window for {asset}", flush=True)
            break

        if error_code == "badResumptionToken":
            # The checkpointed token expired between runs. Restart the whole
            # window cleanly: next write truncates, discarding the partial file.
            print(
                "[arxiv] resumptionToken expired (badResumptionToken); "
                "restarting harvest window from scratch",
                flush=True,
            )
            token = None
            from_date = fresh_from
            until_date = fresh_until
            records_written = 0
            max_datestamp = prior_watermark
            first_write_truncates = True
            continue

        if error_code is not None:
            raise RuntimeError(f"[arxiv] OAI-PMH error '{error_code}' on {asset}")

        if next_token is not None and next_token == used_token:
            raise RuntimeError(
                f"[arxiv] resumptionToken did not advance for {asset} — "
                "server returned the same token; aborting to avoid an infinite loop."
            )

        _write_page(asset, records, truncate=first_write_truncates)
        first_write_truncates = False

        for row in records:
            records_written += 1
            ds = row.get("datestamp")
            if ds and (max_datestamp is None or ds > max_datestamp):
                max_datestamp = ds

        # Raw written; only now checkpoint progress (raw-before-state).
        if next_token:
            save_state(
                asset,
                {
                    "schema_version": STATE_SCHEMA_VERSION,
                    "watermark": prior_watermark,  # unchanged until the run completes
                    "resume": {
                        "token": next_token,
                        "from": from_date,
                        "until": until_date,
                        "records": records_written,
                        "max_datestamp": max_datestamp,
                    },
                },
            )
            token = next_token
            if page % 50 == 0:
                total = f"/{complete_size}" if complete_size else ""
                print(
                    f"[arxiv] page {page}: {records_written}{total} records harvested",
                    flush=True,
                )
        else:
            # Final page reached — harvest complete. Drop the resume checkpoint
            # and advance the watermark.
            token = None
            break

    final_watermark = max_datestamp or prior_watermark
    save_state(
        asset,
        {
            "schema_version": STATE_SCHEMA_VERSION,
            "watermark": final_watermark,
            "last_success_at": datetime.now(timezone.utc).isoformat(),
            "last_run_stats": {
                "records": records_written,
                "pages": page,
                "from": from_date,
                "until": until_date,
                "complete_list_size": complete_size,
            },
        },
    )
    print(
        f"[arxiv] done: {records_written} records over {page} pages, "
        f"watermark={final_watermark}",
        flush=True,
    )


DOWNLOAD_SPECS = [
    NodeSpec(
        id="arxiv-papers",
        fn=fetch_papers,
        args=("papers",),
        deps=(),
        kind="download",
    ),
]
