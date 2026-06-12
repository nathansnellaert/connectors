"""arXiv connector — full paper-metadata corpus via OAI-PMH.

Entity union = ["papers"]. The corpus is ~2.5M+ records growing ~20k/month —
far too large to re-pull in one run — so `papers` is a record-stream firehose
(shape 3): each refresh harvests a bounded slice via OAI-PMH ListRecords
(metadataPrefix=arXiv) and advances a datestamp watermark. Backfill is a
sequence of bounded refreshes driven by the maintain step.

Mechanism (precomputed by research): oai_pmh.
  endpoint   = https://oaipmh.arxiv.org/oai   (new host since March 2025)
  verb       = ListRecords, metadataPrefix=arXiv (split authors, categories,
               license — richer than oai_dc)
  paging     = resumptionToken until exhausted
  incremental= from/until on the OAI datestamp (record MODIFICATION date, not
               original submission date) at YYYY-MM-DD granularity
  pacing     = no documented OAI limit; research recommends a 3s defensive
               delay (same as arXiv's REST guidance) — applied between pages.

Resume model: the watermark is the max OAI datestamp written so far. Within a
run we page via fresh, in-memory resumptionTokens (which expire ~24h, so they
are NEVER persisted across runs). When the per-run time budget is hit we return
cleanly with the watermark advanced; the next run restarts the window from
watermark - OVERLAP. Records are returned in ascending datestamp order, so the
watermark advances monotonically and the overlap-induced duplicates are
dedup'd in the transform (row_number() per arxiv_id). Raw is written before
state, always.
"""
from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone

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
    SqlNodeSpec,
    get,
    load_state,
    save_raw_ndjson,
    save_state,
)

# --- constants ---------------------------------------------------------------

ENDPOINT = "https://oaipmh.arxiv.org/oai"
METADATA_PREFIX = "arXiv"

# OAI harvesting began 2005-09-16 (earliest_datestamp from research). The first
# run, with no watermark, backfills from here forward in bounded slices.
EARLIEST_DATESTAMP = "2005-09-16"

# Re-harvest a few datestamp-days of overlap on every refresh: OAI datestamp is
# day-granular and a run can stop mid-day, so we re-fetch the boundary to avoid
# gaps. Duplicates are dedup'd in the transform.
OVERLAP = timedelta(days=3)

# Soft per-run budget (deliberate pacing — expected to fire on every backfill
# refresh). Hitting it returns cleanly with the watermark advanced; the next
# run resumes. NOT a growth-detecting hard cap.
MAX_FETCH_SECONDS = 1200

# Defensive inter-page delay (research: no documented OAI limit, mirror the 3s
# REST guidance).
PAGE_DELAY_SECONDS = 3.0

STATE_VERSION = 1

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
AX_NS = "http://arxiv.org/OAI/arXiv/"
_NS = {"oai": OAI_NS, "ax": AX_NS}


# --- HTTP retry --------------------------------------------------------------

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
        return code == 429 or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_oai(params: dict) -> bytes:
    """One OAI-PMH GET. Retries transient transport/5xx/429; returns raw XML."""
    resp = get(ENDPOINT, params=params, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.content


# --- parsing -----------------------------------------------------------------

def _txt(el, path):
    found = el.find(path, _NS)
    if found is not None and found.text:
        return found.text.strip()
    return None


def _parse_page(xml_bytes: bytes):
    """Return (rows, resumption_token, oai_error_code).

    oai_error_code is set when the response carries an <error> element (OAI
    reports application errors as a 200 with an error code, not an HTTP status).
    """
    root = etree.fromstring(xml_bytes)

    err = root.find(".//oai:error", _NS)
    if err is not None:
        return [], None, err.get("code")

    rows = []
    for r in root.findall(".//oai:record", _NS):
        hdr = r.find("oai:header", _NS)
        meta = r.find("oai:metadata/ax:arXiv", _NS)
        if meta is None:
            # Deleted record (status="deleted") or otherwise no metadata — skip.
            continue
        authors = []
        for au in meta.findall("ax:authors/ax:author", _NS):
            parts = [
                _txt(au, "ax:forenames"),
                _txt(au, "ax:keyname"),
                _txt(au, "ax:suffix"),
            ]
            name = " ".join(p for p in parts if p)
            if name:
                authors.append(name)
        cats = _txt(meta, "ax:categories")
        primary = cats.split()[0] if cats else None
        rows.append({
            "arxiv_id": _txt(meta, "ax:id"),
            "datestamp": _txt(hdr, "oai:datestamp"),
            "created": _txt(meta, "ax:created"),
            "updated": _txt(meta, "ax:updated"),
            "title": _txt(meta, "ax:title"),
            "abstract": _txt(meta, "ax:abstract"),
            "categories": cats,
            "primary_category": primary,
            "doi": _txt(meta, "ax:doi"),
            "journal_ref": _txt(meta, "ax:journal-ref"),
            "comments": _txt(meta, "ax:comments"),
            "license": _txt(meta, "ax:license"),
            "msc_class": _txt(meta, "ax:msc-class"),
            "acm_class": _txt(meta, "ax:acm-class"),
            "report_no": _txt(meta, "ax:report-no"),
            "authors": authors,
            "set_specs": [s.text for s in hdr.findall("oai:setSpec", _NS) if s.text],
        })

    tok_el = root.find(".//oai:resumptionToken", _NS)
    token = tok_el.text.strip() if (tok_el is not None and tok_el.text) else None
    return rows, token, None


# --- fetch -------------------------------------------------------------------

def fetch_papers(node_id: str) -> None:
    asset_base = node_id  # "arxiv-papers" — batch assets are "<base>-<seq>"
    state_key = node_id

    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        state = {}  # unknown/legacy state shape — reset and re-backfill.

    watermark = state.get("watermark")  # max OAI datestamp written, or None
    batch_seq = int(state.get("batch_seq", 0))

    # Freeze the window for this run.
    if watermark:
        from_date = (date.fromisoformat(watermark) - OVERLAP).isoformat()
    else:
        from_date = EARLIEST_DATESTAMP
    until_date = datetime.now(tz=timezone.utc).date().isoformat()

    params = {
        "verb": "ListRecords",
        "metadataPrefix": METADATA_PREFIX,
        "from": from_date,
        "until": until_date,
    }

    deadline = time.monotonic() + MAX_FETCH_SECONDS
    records_this_run = 0
    pages = 0
    max_datestamp = watermark

    print(
        f"[{asset_base}] harvest from={from_date} until={until_date} "
        f"(watermark={watermark}, batch_seq={batch_seq})",
        flush=True,
    )

    while True:
        xml = _fetch_oai(params)
        rows, token, err = _parse_page(xml)

        if err == "noRecordsMatch":
            # Window empty — nothing modified in [from, until]. Cycle complete;
            # advance the watermark so the next refresh starts at the boundary.
            max_datestamp = max_datestamp or until_date
            print(f"[{asset_base}] noRecordsMatch — window empty", flush=True)
            break
        if err == "badResumptionToken":
            # A fresh in-run token expired or the server rotated state. The
            # watermark is already persisted from the last written page; stop
            # cleanly and let the next run restart the window from there.
            print(f"[{asset_base}] badResumptionToken — stopping, watermark={max_datestamp}", flush=True)
            break
        if err is not None:
            # badArgument / cannotDisseminateFormat / etc. — a programming or
            # config error, not transient. Fail loudly.
            raise RuntimeError(f"OAI-PMH error code={err!r} for params={params}")

        if rows:
            batch_seq += 1
            asset = f"{asset_base}-{batch_seq:08d}"
            # Write raw FIRST, then advance state — a crash in between only
            # re-fetches this batch on resume (idempotent), never a phantom gap.
            save_raw_ndjson(rows, asset)
            records_this_run += len(rows)
            page_max = max(r["datestamp"] for r in rows if r["datestamp"])
            if max_datestamp is None or page_max > max_datestamp:
                max_datestamp = page_max
            save_state(state_key, {
                "schema_version": STATE_VERSION,
                "watermark": max_datestamp,
                "batch_seq": batch_seq,
                "last_run_stats": {
                    "records_this_run": records_this_run,
                    "pages_this_run": pages + 1,
                    "from": from_date,
                    "until": until_date,
                    "last_success_at": datetime.now(tz=timezone.utc).isoformat(),
                },
            })

        pages += 1
        if pages % 25 == 0:
            print(
                f"[{asset_base}] page {pages}: {records_this_run} records, "
                f"watermark={max_datestamp}",
                flush=True,
            )

        if not token:
            print(f"[{asset_base}] resumption exhausted — cycle complete", flush=True)
            # Cycle done: watermark = end of frozen window so the next refresh
            # picks up modifications after `until`.
            max_datestamp = max(max_datestamp or until_date, until_date)
            break

        if time.monotonic() >= deadline:
            print(
                f"[{asset_base}] per-run budget reached after {pages} pages "
                f"({records_this_run} records) — resuming next run from {max_datestamp}",
                flush=True,
            )
            break

        # Subsequent pages: resumptionToken only (no metadataPrefix/from/until).
        params = {"verb": "ListRecords", "resumptionToken": token}
        time.sleep(PAGE_DELAY_SECONDS)

    save_state(state_key, {
        "schema_version": STATE_VERSION,
        "watermark": max_datestamp,
        "batch_seq": batch_seq,
        "last_run_stats": {
            "records_this_run": records_this_run,
            "pages_this_run": pages,
            "from": from_date,
            "until": until_date,
            "last_success_at": datetime.now(tz=timezone.utc).isoformat(),
        },
    })
    print(
        f"[{asset_base}] done: {records_this_run} records over {pages} pages, "
        f"watermark={max_datestamp}",
        flush=True,
    )


DOWNLOAD_SPECS = [
    NodeSpec(id="arxiv-papers", fn=fetch_papers, kind="download"),
]


# --- transform: one published Delta table per subset -------------------------
# Thin parse-and-type pass over the harvested NDJSON batches. The dep view
# globs every "arxiv-papers-*" batch file. Overlap-induced duplicates are
# resolved by keeping the most recently modified row per arxiv_id.

TRANSFORM_SPECS = [
    SqlNodeSpec(
        id="arxiv-papers-transform",
        deps=["arxiv-papers"],
        sql='''
            SELECT
                arxiv_id,
                title,
                abstract,
                primary_category,
                categories,
                authors,
                len(authors)               AS num_authors,
                set_specs,
                TRY_CAST(created AS DATE)   AS created_date,
                TRY_CAST(updated AS DATE)   AS updated_date,
                TRY_CAST(datestamp AS DATE) AS modified_date,
                doi,
                journal_ref,
                comments,
                license,
                msc_class,
                acm_class,
                report_no
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY arxiv_id
                    ORDER BY datestamp DESC, updated DESC NULLS LAST
                ) AS rn
                FROM "arxiv-papers"
                WHERE arxiv_id IS NOT NULL AND title IS NOT NULL
            )
            WHERE rn = 1
        ''',
    ),
]
