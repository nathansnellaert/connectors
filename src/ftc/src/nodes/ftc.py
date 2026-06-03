"""FTC connector — download node for the two api.ftc.gov/v0 list endpoints.

The FTC public API (https://api.ftc.gov/v0) exposes exactly two list endpoints,
each a distinct dataset and its own collect entity:

  - hsr-early-termination-notices : Hart-Scott-Rodino merger early-termination
    notices. JSON:API 1.0 envelope ({jsonapi, data, meta, links}); meta.count
    carries the total (~27.8k as of 2026-06). Paginated with page[offset] /
    page[limit] (max 50, default 50), sorted created DESC, with a links.next
    cursor. Small corpus → STATELESS FULL RE-PULL every refresh (download prompt
    shape 1): ~560 paged requests, overwrite the single raw asset. Revisions and
    new notices are picked up for free because we never trust a stored cursor.

  - dnc-complaints : Do Not Call / robocall complaints. Plain JSON envelope
    ({data, meta, links}); each record carries a monotonically increasing integer
    `seq` (~18.6M as of 2026-06 → millions of rows). Genuinely UNBOUNDED, so a
    full snapshot per refresh is infeasible and the documented created_date_from/
    _to filter is BROKEN on the live API (returns {"data": null}; verified
    2026-06-03). The only usable lever is `seq`: the `offset` query param does NOT
    behave as a row-skip — it behaves as a seq ceiling. `offset=N` returns the up
    to `items_per_page` records with seq <= N, ordered seq DESC (verified live:
    offset=100 -> seq 100,99,98; offset=50000 -> seq 50000,49999,...). So a single
    50-row page at offset=K retrieves exactly the seq window (K-49 .. K). That
    makes `seq` a stable, monotonic, gap-safe cursor. DNC is therefore a
    RECORD-STREAM FIREHOSE (download prompt shape 3): we sweep seq-space in steps
    of 50, write raw in batches, and persist a `watermark` (= seq covered) after
    every batch. Each refresh does a bounded slice; backfill of the full corpus
    spans many refreshes. New complaints (seq > head) are caught on later runs as
    the source head grows.

Raw format: NDJSON (zstd) for both. v0 is "under active development" and the FTC
docs warn the response structure may change, so we store {id, type, **attributes}
per record and let transform re-type on read — no brittle parquet schema.

Auth: api.data.gov key via the `api_key` query param. Read from the FTC_API_KEY
env var (the registered, ~1,000 req/hr production key), falling back to DEMO_KEY
(rate-capped ~30/hr & 50/day) for local probing. 429s are retried with backoff;
if a run exhausts its retries mid-sweep it flushes what it has and returns
cleanly (firehose pacing) so partial progress always persists.

Freshness ("should this run?") is the maintain step's job — if a fetch fn is
invoked, it fetches.
"""
import os
import time

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
    load_state,
    save_state,
)

BASE_URL = "https://api.ftc.gov/v0"

# State schema version for the DNC firehose watermark. Bump when the watermark
# contract changes so stored state from an older shape is discarded.
STATE_VERSION = 1

# DNC firehose tuning. PAGE is the API's hard max (items_per_page <= 50); stepping
# the seq cursor by exactly PAGE guarantees every 50-wide seq window is fully
# retrieved by one page (seq are unique integers, so <= 50 fall in any 50-span).
PAGE = 50
BATCH_RECORDS = 25_000          # flush a raw batch file once the buffer hits this
MAX_REQUESTS_PER_RUN = 1_000    # soft per-run cap (~50k seq / ~50k records)
MAX_FETCH_SECONDS = 1_500       # soft per-run wall-clock cap (~25 min)


# --- transport -------------------------------------------------------------
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


def _api_key() -> str:
    # Registered key in production (CI injects it); DEMO_KEY for local probing.
    return os.environ.get("FTC_API_KEY", "DEMO_KEY")


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _get_json(path: str, params: dict) -> dict:
    """One GET against api.ftc.gov/v0, with the api key folded in. Retries
    transient transport errors / 429 / 5xx with exponential backoff."""
    full = {"api_key": _api_key(), **params}
    resp = get(f"{BASE_URL}/{path}", params=full, timeout=(10.0, 180.0))
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
    rows: list[dict] = []
    offset = 0
    count = None
    pages = 0
    # Safety ceiling: ~560 pages expected; a blown cap means unexpected growth
    # or a pagination loop and should surface loudly, not silently truncate.
    max_pages = 4_000

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
            if pages > max_pages:
                raise RuntimeError(
                    f"[hsr] exceeded {max_pages} pages (count={count}); "
                    "source grew unexpectedly or pagination looped"
                )
            has_next = bool((page.get("links") or {}).get("next"))
            if not data or not has_next:
                break
            offset += PAGE
    except Exception as exc:
        # Transient retries exhausted (or a permanent error) mid-crawl. Persist
        # whatever we fetched so the asset is non-empty and the next refresh
        # re-pulls in full; don't lose a near-complete snapshot to one 429.
        if not rows:
            raise
        print(
            f"[hsr] crawl interrupted after {pages} pages "
            f"({type(exc).__name__}: {exc}); writing partial {len(rows)} rows",
            flush=True,
        )

    save_raw_ndjson(rows, asset)  # overwrite; full snapshot, no watermark
    save_state(asset, {
        "schema_version": STATE_VERSION,
        "last_run_stats": {"records": len(rows), "reported_count": count},
    })


# --- DNC: record-stream firehose over the seq cursor (shape 3) -------------
def _dnc_head_seq() -> int:
    """Current source-side max seq. offset=0 returns the newest page (its
    ordering is not clean DESC, so take the max seq present)."""
    page = _get_json("dnc-complaints", {"items_per_page": PAGE, "offset": 0})
    data = page.get("data") or []
    seqs = [r["attributes"].get("seq") for r in data
            if isinstance(r.get("attributes"), dict) and r["attributes"].get("seq") is not None]
    if not seqs:
        raise RuntimeError("[dnc] could not determine head seq (empty offset=0 page)")
    return max(seqs)


def fetch_dnc(node_id: str) -> None:
    state_key = node_id  # "ftc-dnc-complaints"
    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        if state:
            print(f"[dnc] state schema {state.get('schema_version')} != "
                  f"{STATE_VERSION}; resetting watermark", flush=True)
        state = {}
    watermark = int(state.get("watermark", 0))  # all seq <= watermark fetched

    head_seq = _dnc_head_seq()
    print(f"[dnc] resume watermark={watermark} head_seq={head_seq}", flush=True)
    if watermark >= head_seq:
        print("[dnc] caught up to head; nothing to fetch this run", flush=True)
        return

    buffer: list[dict] = []
    written = 0
    requests = 0
    deadline = time.time() + MAX_FETCH_SECONDS

    def _flush_and_checkpoint() -> None:
        nonlocal buffer, written
        if buffer:
            seqs = [r.get("seq") for r in buffer if r.get("seq") is not None]
            lo, hi = (min(seqs), max(seqs)) if seqs else (watermark, watermark)
            batch_key = f"{lo:010d}-{hi:010d}"  # pure batch coordinate
            save_raw_ndjson(buffer, f"ftc-dnc-complaints-{batch_key}")
            written += len(buffer)
            buffer = []
        # Raw is now current through `watermark`; only then advance persisted state.
        save_state(state_key, {
            "schema_version": STATE_VERSION,
            "watermark": watermark,
            "head_seq": head_seq,
            "last_run_stats": {"records_this_run": written, "requests": requests},
        })

    try:
        while watermark < head_seq and requests < MAX_REQUESTS_PER_RUN:
            if time.time() > deadline:
                print("[dnc] wall-clock budget reached; pausing", flush=True)
                break
            next_offset = watermark + PAGE
            page = _get_json(
                "dnc-complaints",
                {"items_per_page": PAGE, "offset": next_offset},
            )
            requests += 1
            data = page.get("data") or []
            # Keep only records in the new window (seq > watermark); a sparse
            # window returns some already-seen lower seqs as filler — drop them.
            for r in data:
                row = _flatten(r)
                seq = row.get("seq")
                if isinstance(seq, int) and seq > watermark:
                    buffer.append(row)
            watermark = next_offset  # seq-space cursor advances regardless of gaps
            if requests % 50 == 0:
                print(f"[dnc] req {requests} watermark {watermark} "
                      f"buffered {len(buffer)} written {written}", flush=True)
            if len(buffer) >= BATCH_RECORDS:
                _flush_and_checkpoint()
    except Exception as exc:
        # Retries exhausted (rate limit) or a permanent error mid-sweep. Persist
        # progress and return cleanly — firehose pacing; the next run resumes
        # from the saved watermark.
        print(f"[dnc] sweep interrupted at watermark {watermark} "
              f"({type(exc).__name__}: {exc}); checkpointing", flush=True)
        _flush_and_checkpoint()
        return

    _flush_and_checkpoint()
    print(f"[dnc] run done: watermark={watermark} head={head_seq} "
          f"records_this_run={written}", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id="ftc-hsr-early-termination-notices", fn=fetch_hsr, kind="download"),
    NodeSpec(id="ftc-dnc-complaints", fn=fetch_dnc, kind="download"),
]
