"""arXiv download node — OAI-PMH metadata harvest.

Mechanism (chosen by research): OAI-PMH ListRecords, metadataPrefix=arXiv,
endpoint https://oaipmh.arxiv.org/oai (the post-March-2025 host). One entity:
`papers` — the full ~2.5M-record arXiv metadata corpus, growing ~20k/month.

== Endpoint behaviour (probed live, 2026-05-28) ==
The OAI host sits behind Fastly in front of a Google-Frontend origin. Cacheable
verbs (Identify, ListMetadataFormats) answer instantly from the CDN, but every
ListRecords/ListIdentifiers request hits the origin and is SLOW: the origin
generates the response while Fastly's edge times out the first byte (~180s) and
returns "503 first byte timeout". Crucially the origin keeps working and warms
the cache, so RETRYING THE IDENTICAL REQUEST succeeds — probing saw a page warm
on attempt 4 (~200s) or sometimes on attempt 1 (~5-25s). The transient-retry
decorator on `_fetch_root` is therefore load-bearing, not decorative: a single
page routinely needs several attempts before the warm 200 lands.

Because warming is intermittent, a read timeout that survives all retries is an
EXPECTED, NON-FATAL event for this firehose — not a reason to fail the spec.
The crawl catches transient-retry exhaustion at the top level, flushes whatever
it has, advances state, and returns cleanly; the next run resumes. (The prior
implementation let that exception propagate and failed the whole DAG with "read
operation timed out" — that is the bug this version fixes.)

== Resumption / watermark ==
arXiv's resumptionToken is transparent and stateless: its text is literally
  verb=ListRecords&metadataPrefix=arXiv&from=<F>&until=<U>&skip=<N>
i.e. the query plus an integer offset. The server does NOT honour `skip` as a
direct request parameter (badArgument), but it DOES accept a hand-reconstructed
token string — verified live — so we can resume from any offset durably. The
server's tokens carry expirationDate=<next UTC midnight>, but since the token is
stateless we reconstruct it ourselves and never depend on that expiry. The
durable watermark is therefore the integer `skip` offset within a date-bounded
crawl segment.

Date bounding makes the result set immutable while we crawl it: arXiv orders by
id, so a crawl with a frozen `until` sees a fixed set (new submissions get a
datestamp > until and fall outside), keeping offsets stable across the multi-run
backfill. completeListSize is not reported, so pagination terminates only when a
page returns no resumptionToken (or noRecordsMatch).

Backfill = one offset crawl over [SOURCE_MIN, T0] (T0 frozen at first run),
spread across as many bounded runs as the per-run time budget requires. Once a
segment exhausts, the watermark advances to its `until`; refresh starts a fresh
segment over [watermark - overlap, today], catching new and re-stamped records
(datestamp tracks record modification). Overlap duplicates are dedup'd by the
id-keyed merge in transform — they are the safety, not a bug.

Raw format: NDJSON. Records are nested (authors list) with many optional fields
(license, doi, journal-ref, msc-class, comments, ...) that come and go across
records — exactly the drifty/nested shape NDJSON is for.
"""
from __future__ import annotations

import time
import urllib.parse
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
                                   # (it is empty; real records start 2005-09-17)

OVERLAP_DAYS = 7                   # re-harvest tail on refresh (record-mod lag)
BATCH_SIZE = 50_000                # records buffered per NDJSON file (~25MB zstd)
MAX_FETCH_SECONDS = 1500           # per-run soft budget; deliberate pacing, fires
                                   # on every backfill run, returns cleanly
SLEEP_BETWEEN_PAGES = 1.0          # defensive pacing (OAI has no documented limit)
MAX_PAGES_PER_RUN = 5000           # hard safety: ~6.5M records (~2.5x corpus). A
                                   # legit time-bounded run does far fewer; tripping
                                   # this means the token isn't terminating — raise,
                                   # don't loop silently.
STATE_VERSION = 2                  # bumped from 1: watermark contract changed to
                                   # {watermark, crawl:{from,until,skip}}.

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
    etree.XMLSyntaxError,          # truncated/garbled page from a mid-read blip
)


# --- transport -------------------------------------------------------------

def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        # 503 is arXiv/Fastly "first byte timeout" (origin still warming the
        # cache); 429/5xx are transient. Retry the identical request.
        return code == 429 or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(10),
    wait=wait_exponential(min=3, max=45),
    reraise=True,
)
def _fetch_root(params: dict) -> etree._Element:
    """One OAI-PMH request -> parsed XML root. Retried on transient failure.

    Read timeout is 70s: it trips before Fastly's ~180s edge 503, so a cold
    (still-warming) request fails fast and we retry into the now-warmer cache
    rather than burning the budget waiting on a single dead first byte. 10
    attempts gives headroom — probing saw warm-up take up to ~4 attempts.
    """
    resp = get(ENDPOINT, params=params, timeout=(10.0, 70.0))
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
    """-> (rows, resumption_token_text, error_code). error_code is set for an
    OAI <error> element; resumption_token_text is the token's text or None."""
    err = root.find(".//" + _OAI + "error")
    if err is not None:
        return [], None, err.get("code")
    rows = [_parse_record(rec) for rec in root.iter(_OAI + "record")]
    tok = root.find(".//" + _OAI + "resumptionToken")
    token = tok.text if (tok is not None and tok.text) else None
    return rows, token, None


def _parse_skip(token_text: str) -> int | None:
    """Extract the integer `skip` offset from a resumptionToken's text. The
    token is URL-encoded in the XML (skip%3DN); decode then read skip=N."""
    dec = urllib.parse.unquote(token_text)
    for part in dec.split("&"):
        if part.startswith("skip="):
            try:
                return int(part[5:])
            except ValueError:
                return None
    return None


# --- date helpers ----------------------------------------------------------

def _d(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _s(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


# --- fetch -----------------------------------------------------------------

def fetch_papers(entity_id: str) -> None:
    """Harvest the arXiv metadata corpus into NDJSON batches.

    Pages a date-bounded crawl segment by reconstructed offset token, writing
    batches raw-before-state, until the segment exhausts or the per-run budget
    is spent. A transient-retry exhaustion (the endpoint failing to warm within
    the retry budget) flushes and returns cleanly — partial progress, resumed
    next run. State carries the durable `skip` offset plus a date watermark.
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

    watermark = state.get("watermark")          # date through which fully harvested
    crawl = state.get("crawl")                   # in-progress segment, or None

    if not crawl:
        # Start a fresh segment: backfill from SOURCE_MIN on the first run, else
        # a refresh window from watermark-overlap to a frozen `today`.
        frm = (_s(_d(watermark) - timedelta(days=OVERLAP_DAYS))
               if watermark else SOURCE_MIN)
        crawl = {"from": frm, "until": today, "skip": 0}

    seg_from = crawl["from"]
    seg_until = crawl["until"]
    skip = int(crawl.get("skip", 0))

    # Mutable run accumulators.
    buf: list[dict] = []
    seq = 0
    pages = 0
    records = 0

    def flush(cur_skip: int, complete: bool) -> None:
        """Write buffered rows to raw FIRST, then persist state. `complete`
        marks the segment exhausted (advance watermark, clear crawl); otherwise
        persist the resumable offset for the next run."""
        nonlocal buf, seq
        if buf:
            asset = f"arxiv-{entity_id}-{run_ts}-{seq:04d}"
            save_raw_ndjson(buf, asset)
            print(f"[{state_key}] flushed {len(buf)} records -> {asset}", flush=True)
            seq += 1
            buf = []
        if complete:
            new_state = {
                "schema_version": STATE_VERSION,
                "watermark": seg_until,
                "crawl": None,
            }
        else:
            new_state = {
                "schema_version": STATE_VERSION,
                "watermark": watermark,
                "crawl": {"from": seg_from, "until": seg_until, "skip": cur_skip},
            }
        new_state["last_success_at"] = datetime.now(tz=timezone.utc).isoformat()
        new_state["last_run_stats"] = {
            "run_ts": run_ts,
            "records": records,
            "pages": pages,
            "batches": seq,
            "segment": [seg_from, seg_until],
            "skip": cur_skip,
            "complete": complete,
        }
        save_state(state_key, new_state)

    print(f"[{state_key}] crawl segment [{seg_from}, {seg_until}] from skip={skip}",
          flush=True)

    try:
        while True:
            if skip == 0:
                params = {"verb": "ListRecords", "metadataPrefix": METADATA_PREFIX,
                          "from": seg_from, "until": seg_until}
            else:
                # Reconstruct the stateless offset token (durable across the
                # server's midnight expiry — we never use the server's token).
                token = (f"verb=ListRecords&metadataPrefix={METADATA_PREFIX}"
                         f"&from={seg_from}&until={seg_until}&skip={skip}")
                params = {"verb": "ListRecords", "resumptionToken": token}

            root = _fetch_root(params)
            rows, next_token, err = _parse_page(root)

            if err == "noRecordsMatch":
                # Empty segment (e.g. a refresh window with no recent changes).
                flush(skip, complete=True)
                print(f"[{state_key}] noRecordsMatch — segment complete", flush=True)
                return
            if err:
                # badResumptionToken or any other OAI error is unexpected here
                # (we reconstruct stateless tokens). Surface it.
                raise RuntimeError(
                    f"OAI-PMH error '{err}' for segment [{seg_from},{seg_until}] "
                    f"skip={skip}")

            buf.extend(rows)
            records += len(rows)
            pages += 1
            if pages % 10 == 0:
                print(f"[{state_key}] {pages} pages, {records} records, "
                      f"skip={skip}", flush=True)

            if pages > MAX_PAGES_PER_RUN:
                raise RuntimeError(
                    f"exceeded MAX_PAGES_PER_RUN={MAX_PAGES_PER_RUN}; "
                    f"resumptionToken not terminating")

            if next_token is None:
                # Last page of the segment — corpus range exhausted.
                flush(skip, complete=True)
                print(f"[{state_key}] segment complete: {records} records, "
                      f"{pages} pages, watermark={seg_until}", flush=True)
                return

            next_skip = _parse_skip(next_token)
            if next_skip is None:
                next_skip = skip + len(rows)
            if next_skip <= skip:
                raise RuntimeError(
                    f"resumptionToken offset did not advance "
                    f"(skip {skip} -> {next_skip}); aborting")
            skip = next_skip

            # Buffer relief: persist a batch + the live offset mid-crawl.
            if len(buf) >= BATCH_SIZE:
                flush(skip, complete=False)

            if time.monotonic() >= deadline:
                # Soft budget: stop cleanly with the offset persisted. Expected
                # to fire on every backfill run — deliberate pacing, not failure.
                flush(skip, complete=False)
                print(f"[{state_key}] budget reached: {records} records, "
                      f"{pages} pages, resume at skip={skip}", flush=True)
                return

            time.sleep(SLEEP_BETWEEN_PAGES)

    except _TRANSIENT_EXC as exc:
        # The endpoint failed to warm within the retry budget on some page.
        # For this firehose that is expected, non-fatal pacing — flush partial
        # progress and resume next run rather than failing the DAG.
        print(f"[{state_key}] transient exhaustion ({type(exc).__name__}) at "
              f"skip={skip}; flushing {len(buf)} buffered records and stopping",
              flush=True)
        flush(skip, complete=False)
        return
    except httpx.HTTPStatusError as exc:
        # A non-transient HTTP status (4xx other than 429) survived retries.
        # Treat like transient pacing for resume rather than crashing the DAG,
        # but log loudly so a persistent 4xx is visible in run logs.
        print(f"[{state_key}] HTTP {exc.response.status_code} at skip={skip} "
              f"survived retries; flushing and stopping", flush=True)
        flush(skip, complete=False)
        return


DOWNLOAD_SPECS = [
    NodeSpec(id="arxiv-papers", fn=fetch_papers, args=("papers",), deps=(), kind="download"),
]
