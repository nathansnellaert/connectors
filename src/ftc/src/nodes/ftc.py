"""FTC connector — download node for the two api.ftc.gov/v0 list endpoints.

The FTC public API (https://api.ftc.gov/v0) exposes exactly two list endpoints,
each a distinct dataset and its own collect entity:

  - hsr-early-termination-notices : Hart-Scott-Rodino merger early-termination
    notices. JSON:API 1.0 envelope ({jsonapi, data, meta, links}); meta.count is
    a reliable total (27,851 as of 2026-06). Paginated with page[offset] /
    page[limit] (max 50), default sort created DESC. NOTE: links.next exists but
    points at www.ftc.gov WITHOUT the api_key, so we step page[offset] ourselves
    against api.ftc.gov and terminate on offset >= meta.count. Small corpus ->
    STATELESS FULL RE-PULL (download prompt shape 1), overwrite one raw asset.

  - dnc-complaints : Do Not Call / robocall complaints. Plain JSON envelope
    ({data, meta, links}); records carry a monotonic int `seq` (~18.6M as of
    2026-06 -> millions of rows). There is NO links.next cursor and meta carries
    no usable total (meta.record-total is a sample record, not a count). The
    documented created_date_from/_to filter is broken on the live API. The corpus
    is genuinely UNBOUNDED, so a full snapshot is infeasible at 50/page. We keep a
    bounded RECENT WINDOW per refresh (download prompt shape (e)/snapshot): step
    `offset` from the newest page, dedupe by id, overwrite one raw asset. Because
    transform merges by record id, each refresh's recent window accumulates new
    complaints over time. Offset semantics on this endpoint are quirky (it behaves
    more like a seq ceiling than a strict row-skip), but in-memory dedupe by id
    makes the window robust to whatever overlap stepping produces.

Raw format: NDJSON (zstd) for both. HSR attributes include a nested list
(`acquired-entities`) and v0 is "under active development" (FTC docs warn the
response structure may change), so we store {id, type, **attributes} per record
and let transform re-type on read -- no brittle parquet schema.

Auth & rate limits (the dominant design constraint here):
  api.data.gov key via the `api_key` query param. Read FTC_API_KEY from the env
  (a registered key, ~1,000 req/hr) and fall back to the shared DEMO_KEY. DEMO_KEY
  is rate-limited PER IP, not per key, and in practice is brutal: observed
  x-ratelimit-limit=10 with a multi-hour Retry-After once exhausted. Both specs
  run sequentially in one CI job behind one IP, so they share that tiny budget.
  Two consequences encoded below:
    1. Per-run page caps are KEY-AWARE. With a registered key we pull HSR in full
       (~558 pages) and a large DNC window; with only DEMO_KEY we fetch a handful
       of pages per spec so both stay non-empty within ~10 requests.
    2. 429 is NOT retried. Its Retry-After is hours, so retrying cannot succeed
       within a run and would only burn more of the shared budget. The retry
       decorator handles genuine transients (timeouts, 5xx); a 429 is caught in
       the caller, which writes whatever was fetched and returns cleanly.

Freshness ("should this run?") is the maintain step's job -- if a fetch fn is
invoked, it fetches.
"""
import os

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import (
    NodeSpec,
    get,
    save_raw_ndjson,
    save_state,
)

BASE_URL = "https://api.ftc.gov/v0"

# API hard max for items_per_page / page[limit].
PAGE = 50

# Per-run page caps. Key-aware: a registered key gets generous pulls; DEMO_KEY
# (the only key available in CI) is throttled to a few pages per spec so both
# specs together stay inside the tiny shared per-IP budget and both land
# non-empty raw. With a key these caps act as safety/pacing, not truncation.
DEMO_MAX_PAGES = 2            # ~100 records/spec when running on DEMO_KEY
HSR_MAX_PAGES_KEYED = None    # None = pull HSR to completion (terminate on count)
DNC_MAX_PAGES_KEYED = 200     # ~10k newest complaints per refresh (bounded window)

# Runaway guard for the HSR full pull. ~558 pages expected; blowing this means
# unexpected source growth or a pagination loop and should surface loudly.
HSR_SAFETY_PAGES = 4000


# --- transport -------------------------------------------------------------
# Genuinely transient transport faults that backoff can recover from. 429 is
# DELIBERATELY excluded: this source returns a multi-hour Retry-After on 429, so
# retrying cannot succeed within a run and only drains the scarce shared budget.
# 429 is handled in the callers instead (write partial, stop cleanly).
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
        # 5xx only -- 429 is handled out-of-band (see note above).
        return 500 <= exc.response.status_code < 600
    return False


def _is_rate_limited(exc: BaseException) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code == 429
    )


def _api_key() -> str:
    # Registered key in production (injected via env); DEMO_KEY otherwise.
    return os.environ.get("FTC_API_KEY") or "DEMO_KEY"


def _has_registered_key() -> bool:
    return bool(os.environ.get("FTC_API_KEY"))


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=2, max=60),
    reraise=True,
)
def _get_json(path: str, params: dict) -> dict:
    """One GET against api.ftc.gov/v0 with the api key folded in. Retries genuine
    transient transport errors / 5xx with exponential backoff; 429 and other 4xx
    propagate immediately for the caller to classify."""
    full = {"api_key": _api_key(), **params}
    resp = get(f"{BASE_URL}/{path}", params=full, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


def _flatten(rec: dict) -> dict:
    """Record -> flat dict tolerant of v0 schema drift: id + type + all
    attributes. NDJSON stores it verbatim; transform re-types on read."""
    return {
        "id": rec.get("id"),
        "type": rec.get("type"),
        **(rec.get("attributes") or {}),
    }


# --- HSR: stateless full re-pull (shape 1) ---------------------------------
def fetch_hsr(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    keyed = _has_registered_key()
    soft_cap = HSR_MAX_PAGES_KEYED if keyed else DEMO_MAX_PAGES  # None = no cap

    rows: list[dict] = []
    count = None
    offset = 0
    pages = 0
    stopped_early = None

    try:
        while True:
            page = _get_json(
                "hsr-early-termination-notices",
                {"page[offset]": offset, "page[limit]": PAGE},
            )
            if count is None:
                count = (page.get("meta") or {}).get("count")
            data = page.get("data") or []
            rows.extend(_flatten(r) for r in data)
            pages += 1
            if pages % 50 == 0:
                print(f"[hsr] page {pages} offset {offset} rows {len(rows)}", flush=True)

            # Runaway guard: raise (don't silently truncate) on unexpected growth.
            if pages >= HSR_SAFETY_PAGES:
                raise RuntimeError(
                    f"[hsr] exceeded {HSR_SAFETY_PAGES} pages (count={count}); "
                    "source grew unexpectedly or pagination looped"
                )

            # Natural end of the finite corpus.
            if not data or (count is not None and offset + PAGE >= count):
                break
            # Deliberate per-run pacing on DEMO_KEY: clean stop, not truncation
            # of a known-finite set (the maintain/next-run picks up the rest).
            if soft_cap is not None and pages >= soft_cap:
                stopped_early = f"soft page cap {soft_cap} (DEMO budget)"
                break
            offset += PAGE
    except Exception as exc:  # noqa: BLE001 -- classified below
        if _is_rate_limited(exc):
            stopped_early = "429 rate limit (shared DEMO budget exhausted)"
        elif rows:
            # Transient retries exhausted (or other error) mid-crawl, but we have
            # data: persist the partial snapshot rather than lose it; next refresh
            # re-pulls in full.
            stopped_early = f"{type(exc).__name__}: {exc}"
        else:
            # Nothing fetched at all -> genuine failure, surface it.
            print(f"[hsr] failed before any data: {type(exc).__name__}: {exc}", flush=True)
            raise

    if stopped_early:
        print(f"[hsr] stopped early after {pages} pages ({len(rows)} rows): "
              f"{stopped_early}", flush=True)

    save_raw_ndjson(rows, asset)  # overwrite; full (or bounded) snapshot
    save_state(asset, {
        "last_run_stats": {
            "records": len(rows),
            "reported_count": count,
            "pages": pages,
            "complete": stopped_early is None,
        },
    })
    print(f"[hsr] wrote {len(rows)} rows (reported total {count})", flush=True)


# --- DNC: bounded recent-window snapshot (shape (e)) -----------------------
def fetch_dnc(node_id: str) -> None:
    asset = node_id
    keyed = _has_registered_key()
    soft_cap = DNC_MAX_PAGES_KEYED if keyed else DEMO_MAX_PAGES

    by_id: dict[str, dict] = {}   # dedupe by record id across stepped windows
    no_id: list[dict] = []        # records without an id (shouldn't happen)
    offset = 0
    pages = 0
    stopped_early = None

    try:
        while pages < soft_cap:
            page = _get_json(
                "dnc-complaints",
                {"items_per_page": PAGE, "offset": offset},
            )
            data = page.get("data") or []
            pages += 1
            if not data:
                stopped_early = "empty page (reached end of available window)"
                break
            new_this_page = 0
            for r in data:
                row = _flatten(r)
                rid = row.get("id")
                if rid is None:
                    no_id.append(row)
                    new_this_page += 1
                elif rid not in by_id:
                    by_id[rid] = row
                    new_this_page += 1
            if pages % 50 == 0:
                print(f"[dnc] page {pages} offset {offset} unique {len(by_id)}", flush=True)
            # If a stepped window returned nothing new, offset stepping has stopped
            # yielding fresh records -- stop rather than spin.
            if new_this_page == 0:
                stopped_early = "no new records this page (window exhausted)"
                break
            offset += PAGE
    except Exception as exc:  # noqa: BLE001 -- classified below
        rows_so_far = len(by_id) + len(no_id)
        if _is_rate_limited(exc):
            stopped_early = "429 rate limit (shared DEMO budget exhausted)"
        elif rows_so_far:
            stopped_early = f"{type(exc).__name__}: {exc}"
        else:
            print(f"[dnc] failed before any data: {type(exc).__name__}: {exc}", flush=True)
            raise

    rows = list(by_id.values()) + no_id
    if stopped_early:
        print(f"[dnc] stopped after {pages} pages ({len(rows)} rows): "
              f"{stopped_early}", flush=True)

    save_raw_ndjson(rows, asset)  # overwrite; transform id-merges across refreshes
    seqs = [r.get("seq") for r in rows if isinstance(r.get("seq"), int)]
    save_state(asset, {
        "last_run_stats": {
            "records": len(rows),
            "pages": pages,
            "max_seq": max(seqs) if seqs else None,
            "min_seq": min(seqs) if seqs else None,
        },
    })
    print(f"[dnc] wrote {len(rows)} unique rows over {pages} pages", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id="ftc-hsr-early-termination-notices", fn=fetch_hsr, kind="download"),
    NodeSpec(id="ftc-dnc-complaints", fn=fetch_dnc, kind="download"),
]
