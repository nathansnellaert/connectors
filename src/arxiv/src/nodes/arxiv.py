"""arXiv download node — OAI-PMH metadata harvest.

Mechanism (chosen by research): OAI-PMH ListRecords, metadataPrefix=arXiv,
endpoint https://oaipmh.arxiv.org/oai (the post-March-2025 host). One entity:
`papers` — the full ~2.5M-record arXiv metadata corpus, growing ~20k/month.

== Endpoint behaviour (probed live, 2026-05-30) ==
ListRecords against the new host answers in ~5-6s per page (1300 records/page),
behind Fastly + a Google-Frontend origin. Cacheable verbs (Identify,
ListMetadataFormats) answer instantly. The origin can occasionally be slow to
warm a cold page and Fastly may return a 503 "first byte timeout" — so the
transient-retry decorator on `_fetch_root` is load-bearing: it retries the
identical (stateless) request into the now-warmer cache. A read-timeout that
survives all retries is an EXPECTED, NON-FATAL event for this firehose: the
crawl flushes what it has, advances state, and returns cleanly; the next run
resumes. It does NOT fail the spec.

== Resumption / watermark (CORRECTED) ==
arXiv's resumptionToken is transparent and stateless. Its decoded text is:
    verb=ListRecords&metadataPrefix=arXiv&from=<F'>&until=<U>&skip=<N>
where F' is an ADVANCING datestamp cursor and N is an offset RELATIVE TO F'
(NOT relative to the segment's original `from`). Probed live: a page starting
from=2024-01-01 returns a token from=2024-01-03&skip=77, the next from=2024-01-05
&skip=343. The token's (from, skip) pair must be carried together — using the
original segment `from` with the server's `skip` (the prior implementation's
bug) requests the wrong window and re-reads / corrupts the crawl.

The token is replayed by RECONSTRUCTING it from the parsed (from, skip) — verified
live to return byte-identical records to the server's own token, and independent
of the server's midnight `expirationDate` (the token is stateless, so a
hand-built string is honoured regardless of the stamped expiry). The durable
watermark is therefore the in-progress segment's {from, until, skip}.

`until` is frozen at segment start, which makes the result set immutable while we
crawl it across many bounded runs (new submissions get a datestamp > until and
fall outside). completeListSize is not reported, so pagination terminates only
when a page returns no resumptionToken (or noRecordsMatch).

Backfill = one segment crawl over [SOURCE_MIN, T0] (T0 = the date frozen at the
first run's `until`), spread across as many bounded per-run budgets as needed.
When a segment exhausts, the watermark advances to its `until`; the next refresh
starts a fresh segment over [watermark - overlap, today], catching new and
re-stamped records (datestamp tracks record modification). Overlap duplicates
are dedup'd by transform's id-keyed merge — they are the safety, not a bug.

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

OVERLAP_DAYS = 7                   # re-harvest tail on refresh (record-mod lag)
BATCH_SIZE = 20_000                # records buffered per NDJSON file (~bounded RAM)
MAX_FETCH_SECONDS = 120            # per-run soft budget; deliberate pacing, fires
                                   # on every backfill run, returns cleanly. Kept
                                   # short so the whole connector run finishes well
                                   # inside the harness's per-step wall-clock — a
                                   # long budget is what orphaned the prior attempt.
SLEEP_BETWEEN_PAGES = 1.0          # defensive pacing (OAI has no documented limit)
MAX_PAGES_PER_RUN = 10_000         # hard safety: a time-bounded run does far fewer;
                                   # tripping this means the token isn't terminating
                                   # — raise, don't loop silently.
STATE_VERSION = 3                  # bumped from 2: the resume contract changed —
                                   # `crawl.from` now tracks the token's ADVANCING
                                   # cursor, not the segment's original from.

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
    stop=stop_after_attempt(8),
    wait=wait_exponential(min=3, max=45),
    reraise=True,
)
def _fetch_root(params: dict) -> etree._Element:
    """One OAI-PMH request -> parsed XML root. Retried on transient failure.

    Read timeout is 90s: it trips before Fastly's ~180s edge 503, so a cold
    (still-warming) request fails fast and we retry into the now-warmer cache
    rather than burning the budget on a single dead first byte.
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
    """-> (rows, resumption_token_text, error_code). error_code is set for an
    OAI <error> element; resumption_token_text is the token's text or None."""
    err = root.find(".//" + _OAI + "error")
    if err is not None:
        return [], None, err.get("code")
    rows = [_parse_record(rec) for rec in root.iter(_OAI + "record")]
    tok = root.find(".//" + _OAI + "resumptionToken")
    token = tok.text if (tok is not None and tok.text) else None
    return rows, token, None


def _parse_token(token_text: str) -> dict | None:
    """Decode a resumptionToken into its {from, until, skip} fields. Returns
    None if the token is malformed (missing the offset). The token is
    URL-encoded in the XML (skip%3DN&from%3D...); unquote then split."""
    dec = urllib.parse.unquote(token_text)
    fields: dict[str, str] = {}
    for part in dec.split("&"):
        if "=" in part:
            k, v = part.split("=", 1)
            fields[k] = v
    if "skip" not in fields:
        return None
    try:
        skip = int(fields["skip"])
    except ValueError:
        return None
    return {
        "from": fields.get("from"),
        "until": fields.get("until"),
        "skip": skip,
    }


def _reconstruct_token(frm: str, until: str, skip: int) -> str:
    """Build a stateless resumptionToken string from its components. Verified
    live to return byte-identical records to the server's own token, and
    independent of the server's stamped midnight expiry."""
    return (f"verb=ListRecords&metadataPrefix={METADATA_PREFIX}"
            f"&from={frm}&until={until}&skip={skip}")


# --- date helpers ----------------------------------------------------------

def _d(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _s(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


# --- fetch -----------------------------------------------------------------

def fetch_papers(entity_id: str) -> None:
    """Harvest the arXiv metadata corpus into NDJSON batches.

    Pages a date-bounded crawl segment, carrying the token's advancing
    (from, skip) cursor in state, writing batches raw-before-state, until the
    segment exhausts or the per-run budget is spent. A transient-retry
    exhaustion (the endpoint failing to warm within the retry budget) flushes
    and returns cleanly — partial progress, resumed next run.
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

    seg_until = crawl["until"]                   # frozen for the whole segment
    cur_from = crawl["from"]                     # advancing cursor (token's `from`)
    skip = int(crawl.get("skip", 0))

    # Mutable run accumulators.
    buf: list[dict] = []
    seq = 0
    pages = 0
    records = 0

    def flush(c_from: str, c_skip: int, complete: bool) -> None:
        """Write buffered rows to raw FIRST, then persist state. `complete`
        marks the segment exhausted (advance watermark, clear crawl); otherwise
        persist the resumable advancing (from, skip) for the next run."""
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
                "crawl": {"from": c_from, "until": seg_until, "skip": c_skip},
            }
        new_state["last_success_at"] = datetime.now(tz=timezone.utc).isoformat()
        new_state["last_run_stats"] = {
            "run_ts": run_ts,
            "records": records,
            "pages": pages,
            "batches": seq,
            "segment": [crawl["from"], seg_until],
            "resume_from": c_from,
            "resume_skip": c_skip,
            "complete": complete,
        }
        save_state(state_key, new_state)

    print(f"[{state_key}] crawl segment until={seg_until} resume from={cur_from} "
          f"skip={skip}", flush=True)

    try:
        while True:
            if skip == 0:
                # No within-date offset: a plain {from, until} request is exactly
                # equivalent to the token from=cur_from&until&skip=0, so use the
                # raw params (covers both the segment's first page and any later
                # page whose cursor lands on skip=0 at a new date).
                params = {"verb": "ListRecords", "metadataPrefix": METADATA_PREFIX,
                          "from": cur_from, "until": seg_until}
            else:
                params = {"verb": "ListRecords",
                          "resumptionToken": _reconstruct_token(
                              cur_from, seg_until, skip)}

            root = _fetch_root(params)
            rows, next_token, err = _parse_page(root)

            if err == "noRecordsMatch":
                # Empty segment (e.g. a refresh window with no recent changes).
                flush(cur_from, skip, complete=True)
                print(f"[{state_key}] noRecordsMatch — segment complete", flush=True)
                return
            if err:
                # badResumptionToken or any other OAI error is unexpected here
                # (we reconstruct stateless tokens). Surface it.
                raise RuntimeError(
                    f"OAI-PMH error '{err}' for segment until={seg_until} "
                    f"from={cur_from} skip={skip}")

            buf.extend(rows)
            records += len(rows)
            pages += 1
            if pages % 10 == 0:
                print(f"[{state_key}] {pages} pages, {records} records, "
                      f"from={cur_from} skip={skip}", flush=True)

            if pages > MAX_PAGES_PER_RUN:
                raise RuntimeError(
                    f"exceeded MAX_PAGES_PER_RUN={MAX_PAGES_PER_RUN}; "
                    f"resumptionToken not terminating")

            if next_token is None:
                # Last page of the segment — corpus range exhausted.
                flush(cur_from, skip, complete=True)
                print(f"[{state_key}] segment complete: {records} records, "
                      f"{pages} pages, watermark={seg_until}", flush=True)
                return

            parsed = _parse_token(next_token)
            if parsed is None:
                raise RuntimeError(
                    f"could not parse resumptionToken offset: {next_token!r}")
            new_from = parsed["from"] or cur_from
            new_skip = parsed["skip"]
            # Sanity: the cursor must make forward progress, else we'd loop.
            if (new_from, new_skip) == (cur_from, skip):
                raise RuntimeError(
                    f"resumptionToken did not advance "
                    f"(from={cur_from} skip={skip}); aborting")
            cur_from, skip = new_from, new_skip

            # Buffer relief: persist a batch + the live cursor mid-crawl.
            if len(buf) >= BATCH_SIZE:
                flush(cur_from, skip, complete=False)

            if time.monotonic() >= deadline:
                # Soft budget: stop cleanly with the cursor persisted. Expected
                # to fire on every backfill run — deliberate pacing, not failure.
                flush(cur_from, skip, complete=False)
                print(f"[{state_key}] budget reached: {records} records, "
                      f"{pages} pages, resume from={cur_from} skip={skip}",
                      flush=True)
                return

            time.sleep(SLEEP_BETWEEN_PAGES)

    except _TRANSIENT_EXC as exc:
        # The endpoint failed to warm within the retry budget on some page.
        # For this firehose that is expected, non-fatal pacing — flush partial
        # progress and resume next run rather than failing the DAG.
        print(f"[{state_key}] transient exhaustion ({type(exc).__name__}) at "
              f"from={cur_from} skip={skip}; flushing {len(buf)} buffered "
              f"records and stopping", flush=True)
        flush(cur_from, skip, complete=False)
        return
    except httpx.HTTPStatusError as exc:
        # A non-transient HTTP status (4xx other than 429) survived retries.
        # Flush partial progress and stop; log loudly so a persistent 4xx is
        # visible in run logs rather than crashing the whole DAG.
        print(f"[{state_key}] HTTP {exc.response.status_code} at from={cur_from} "
              f"skip={skip} survived retries; flushing and stopping", flush=True)
        flush(cur_from, skip, complete=False)
        return


DOWNLOAD_SPECS = [
    NodeSpec(id="arxiv-papers", fn=fetch_papers, args=("papers",), deps=(), kind="download"),
]
