"""arXiv download node — OAI-PMH metadata harvest.

Mechanism (chosen by research): OAI-PMH ListRecords, metadataPrefix=arXiv,
endpoint https://oaipmh.arxiv.org/oai (the post-March-2025 host). One entity:
`papers` — the full ~2.5M-record arXiv metadata corpus, growing ~20k/month.

Why date-chunked harvest (not one continuous resumptionToken crawl):
  * The endpoint is slow — metadata pages take ~8-10s each, so a full backfill
    is ~1900 pages ≈ 4-5h. That cannot finish in a single refresh, so backfill
    is spread across many bounded runs.
  * resumptionTokens are offset-based (`skip=N`) and expire at next UTC midnight,
    so they are NOT a durable cross-run resume point on their own.
  * The harvest is ordered by id, and datestamps are NOT monotonic across pages
    (the entire pre-2005 corpus shares datestamp 2005-09-17 — ~94.5k records in
    one day), so a flushed batch's max datestamp is not a safe restart point.
  Therefore the durable watermark is a fully-harvested DATE boundary: we harvest
  closed date windows [from, until], page each window to completion within a run,
  and advance the watermark only past windows whose records are already on disk.
  A token is persisted for intra-window resume within its validity; if it has
  expired (badResumptionToken) we simply re-harvest the current window from its
  start — bounded by one window, never the whole corpus, so no livelock.

Refresh is the same code with a populated watermark: from = watermark - overlap,
which yields one small window that completes in seconds. Duplicates produced by
the overlap (and by re-harvesting a partial window on resume) are dedup'd by the
id-keyed merge in transform — they are the safety, not a bug.

Raw format: NDJSON. Records are nested (authors list) with many optional fields
(license, doi, journal-ref, msc-class, comments, ...) that come and go across
records — exactly the drifty/nested shape NDJSON is for.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import httpx
from lxml import etree
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, load_state, save_state, save_raw_ndjson

# --- constants -------------------------------------------------------------

ENDPOINT = "https://oaipmh.arxiv.org/oai"
METADATA_PREFIX = "arXiv"
SOURCE_MIN = "2005-09-16"          # earliestDatestamp from the Identify verb

OVERLAP_DAYS = 7                   # re-harvest tail on refresh (record-mod lag)
WINDOW_DAYS = 30                   # date-window span per backfill chunk
BATCH_SIZE = 50_000                # records buffered per NDJSON file (~100MB)
MAX_FETCH_SECONDS = 1500           # per-run soft budget; clears the 2005-09-17
                                   # giant day (~13 min) with margin, then stops
SLEEP_BETWEEN_PAGES = 2.0          # defensive pacing (OAI has no documented limit)
MAX_PAGES_PER_RUN = 4000           # hard safety: ~5.2M records (~2x corpus). A
                                   # legit run is time-bounded to ~130 pages; if
                                   # we ever blow past this the token isn't
                                   # terminating — surface it, don't loop.
STATE_VERSION = 1

_OAI = "{http://www.openarchives.org/OAI/2.0/}"
_ARX = "{http://arxiv.org/OAI/arXiv/}"

_TRANSIENT_EXC = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
    httpx.ProxyError,
)


# --- transport -------------------------------------------------------------

def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        # 503 is OAI-PMH flow control ("retry later"); 429/5xx are transient.
        return code == 429 or 500 <= code < 600
    # A truncated/garbled page parses as malformed XML — treat as transient so
    # a network blip mid-read is retried rather than crashing the spec.
    if isinstance(exc, etree.XMLSyntaxError):
        return True
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(8),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_root(params: dict) -> etree._Element:
    """One OAI-PMH request -> parsed XML root. Retried on transient failure.

    Read timeout is 90s: probing showed real responses land in 14-40s, while
    the Varnish front frequently leaves connections hung — failing those fast
    and retrying beats burning the run budget on a single dead socket. 8
    attempts gives headroom through the multi-minute 503 "first byte timeout"
    spells this endpoint exhibits under load.
    """
    resp = get(ENDPOINT, params=params, timeout=(10.0, 90.0))
    resp.raise_for_status()
    return etree.fromstring(resp.content)


# --- parsing ---------------------------------------------------------------

def _parse_record(rec: etree._Element) -> dict:
    hdr = rec.find(_OAI + "header")
    identifier = hdr.findtext(_OAI + "identifier")
    datestamp = hdr.findtext(_OAI + "datestamp")
    sets = [e.text for e in hdr.findall(_OAI + "setSpec")]
    # arxiv_id without the OAI scheme prefix; recovered from the header for
    # deleted records (which carry no metadata block).
    arxiv_id = (identifier or "").replace("oai:arXiv.org:", "") or None

    if hdr.get("status") == "deleted":
        return {
            "arxiv_id": arxiv_id,
            "oai_identifier": identifier,
            "datestamp": datestamp,
            "sets": sets,
            "deleted": True,
        }

    meta = rec.find(_OAI + "metadata")
    arx = meta.find(_ARX + "arXiv") if meta is not None else None
    if arx is None:
        # Non-deleted record without a metadata block — unexpected; keep the
        # header so transform/health-tests can see it rather than dropping it.
        return {
            "arxiv_id": arxiv_id,
            "oai_identifier": identifier,
            "datestamp": datestamp,
            "sets": sets,
            "deleted": False,
        }

    def txt(tag: str):
        v = arx.findtext(_ARX + tag)
        return v.strip() if isinstance(v, str) else v

    authors = []
    for a in arx.findall(_ARX + "authors/" + _ARX + "author"):
        affil = [e.text for e in a.findall(_ARX + "affiliation") if e.text]
        authors.append({
            "keyname": a.findtext(_ARX + "keyname"),
            "forenames": a.findtext(_ARX + "forenames"),
            "suffix": a.findtext(_ARX + "suffix"),
            "affiliation": affil or None,
        })

    return {
        "arxiv_id": txt("id") or arxiv_id,
        "oai_identifier": identifier,
        "datestamp": datestamp,
        "sets": sets,
        "deleted": False,
        "created": txt("created"),
        "updated": txt("updated"),
        "title": txt("title"),
        "abstract": txt("abstract"),
        "categories": txt("categories"),
        "comments": txt("comments"),
        "journal_ref": txt("journal-ref"),
        "report_no": txt("report-no"),
        "doi": txt("doi"),
        "msc_class": txt("msc-class"),
        "acm_class": txt("acm-class"),
        "license": txt("license"),
        "proxy": txt("proxy"),
        "authors": authors,
    }


def _parse_page(root: etree._Element) -> tuple[list[dict], str | None, str | None]:
    """-> (rows, resumption_token, error_code). error_code is set for OAI <error>."""
    err = root.find(".//" + _OAI + "error")
    if err is not None:
        return [], None, err.get("code")
    rows = [_parse_record(rec) for rec in root.iter(_OAI + "record")]
    tok = root.find(".//" + _OAI + "resumptionToken")
    token = tok.text if (tok is not None and tok.text) else None
    return rows, token, None


# --- date helpers ----------------------------------------------------------

def _d(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _s(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


# --- fetch -----------------------------------------------------------------

def fetch_papers(entity_id: str) -> None:
    """Harvest the arXiv metadata corpus into NDJSON batches.

    Walks closed date windows forward from the durable watermark, paging each
    window to completion, until it reaches today or the per-run time budget is
    spent. State advances only past windows whose records are already on disk.
    """
    state_key = f"arxiv-{entity_id}"
    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        if state:
            print(f"[{state_key}] state schema_version "
                  f"{state.get('schema_version')!r} != {STATE_VERSION}; resetting",
                  flush=True)
        state = {}

    run_ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    today = _s(datetime.now(tz=timezone.utc))
    deadline = time.monotonic() + MAX_FETCH_SECONDS

    # Durable progress: every datestamp <= watermark is fully on disk.
    watermark = state.get("watermark")
    # In-progress (partially harvested) window carried from a prior run.
    cursor = state.get("cursor")
    win_from = state.get("window_from")
    win_until = state.get("window_until")

    # Mutable run accumulators.
    buf: list[dict] = []
    seq = 0
    pages = 0
    records = 0
    # pending_watermark: until of the latest window fully harvested INTO buf.
    # Never written to state until buf is flushed (raw-before-state).
    pending_watermark = watermark

    def flush(cur_token: str | None, cur_from: str | None, cur_until: str | None) -> None:
        """Write buffered rows to raw FIRST, then persist state. cur_* describe
        the still-in-progress window (None when nothing is pending)."""
        nonlocal buf, seq
        if buf:
            asset = f"arxiv-{entity_id}-{run_ts}-{seq:04d}"
            save_raw_ndjson(buf, asset)
            print(f"[{state_key}] flushed {len(buf)} records -> {asset}", flush=True)
            seq += 1
            buf = []
        save_state(state_key, {
            "schema_version": STATE_VERSION,
            "watermark": pending_watermark,
            "cursor": cur_token,
            "window_from": cur_from,
            "window_until": cur_until,
            "last_success_at": datetime.now(tz=timezone.utc).isoformat(),
            "last_run_stats": {
                "run_ts": run_ts,
                "records": records,
                "pages": pages,
                "batches": seq,
                "watermark": pending_watermark,
            },
        })

    def harvest_window(w_from: str, w_until: str, resume_token: str | None) -> str:
        """Page one window to completion or budget. Appends to buf, flushing at
        BATCH_SIZE. Returns 'complete' | 'budget' | 'expired'."""
        nonlocal pages, records, buf
        token = resume_token
        while True:
            if token:
                params = {"verb": "ListRecords", "resumptionToken": token}
            else:
                params = {"verb": "ListRecords", "metadataPrefix": METADATA_PREFIX,
                          "from": w_from, "until": w_until}
            root = _fetch_root(params)
            rows, next_token, err = _parse_page(root)

            if err == "badResumptionToken":
                # Token expired/invalid — restart THIS window from its start.
                print(f"[{state_key}] resumptionToken expired in [{w_from},{w_until}]; "
                      f"re-harvesting window from start", flush=True)
                return "expired"
            if err == "noRecordsMatch":
                # Empty window — nothing to fetch; treat as complete.
                return "complete"
            if err:
                raise RuntimeError(f"OAI-PMH error '{err}' for window [{w_from},{w_until}]")

            buf.extend(rows)
            records += len(rows)
            pages += 1
            if pages % 10 == 0:
                print(f"[{state_key}] {pages} pages, {records} records "
                      f"(window [{w_from},{w_until}])", flush=True)

            if pages > MAX_PAGES_PER_RUN:
                raise RuntimeError(
                    f"exceeded MAX_PAGES_PER_RUN={MAX_PAGES_PER_RUN}; "
                    f"resumptionToken not terminating")
            if next_token is not None and next_token == token:
                raise RuntimeError("resumptionToken did not advance; aborting loop")

            if next_token is None:
                return "complete"               # window exhausted

            # Buffer relief mid-window: persist what we have and the live token.
            if len(buf) >= BATCH_SIZE:
                flush(next_token, w_from, w_until)

            token = next_token
            if time.monotonic() >= deadline:
                # Soft budget: stop cleanly with the live token persisted.
                flush(token, w_from, w_until)
                return "budget"
            time.sleep(SLEEP_BETWEEN_PAGES)

    # --- resume an in-progress window from a prior run ---------------------
    if cursor and win_from and win_until:
        outcome = harvest_window(win_from, win_until, cursor)
        if outcome == "expired":
            outcome = harvest_window(win_from, win_until, None)
        if outcome == "budget":
            return                              # flush already persisted state
        # complete: data through win_until is buffered; advance and continue.
        pending_watermark = win_until
        cursor = None
        start = _s(_d(win_until) + timedelta(days=1))
    else:
        cursor = None
        start = (_s(_d(watermark) - timedelta(days=OVERLAP_DAYS))
                 if watermark else SOURCE_MIN)

    # --- walk closed date windows forward ---------------------------------
    cur = start
    while cur <= today:
        if time.monotonic() >= deadline:
            break
        w_until = min(_s(_d(cur) + timedelta(days=WINDOW_DAYS - 1)), today)
        outcome = harvest_window(cur, w_until, None)
        if outcome == "expired":
            outcome = harvest_window(cur, w_until, None)  # one bounded retry
        if outcome == "budget":
            return                              # flush already persisted state
        # window complete — its records are in buf; record progress.
        pending_watermark = w_until
        cur = _s(_d(w_until) + timedelta(days=1))

    # --- harvest reached `today` (or budget broke the loop after a complete
    #     window): flush the buffered tail and persist final state. ----------
    flush(None, None, None)
    print(f"[{state_key}] done: {records} records, {pages} pages, "
          f"watermark={pending_watermark}", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id="arxiv-papers", fn=fetch_papers, args=("papers",), deps=(), kind="download"),
]
