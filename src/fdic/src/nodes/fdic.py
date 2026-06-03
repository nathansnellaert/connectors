"""FDIC BankFind Suite download nodes.

Eight endpoints of the FDIC BankFind Suite REST API (https://api.fdic.gov/banks),
each treated as one logical dataset:

    /institutions  (~27.8k)   /locations    (~78k)
    /summary       (~8.1k)    /failures     (~4.1k)
    /history       (~583k)    /demographics (~1.67M)
    /financials    (~1.68M)   /sod          (~2.82M)

Access strategy (per research handoff): offset/limit pagination, max limit 10000
per request. Each response is an envelope ``{meta, data, totals}`` where every
element of ``data`` wraps the real record under ``row["data"]`` (plus an ES
``score``). ``totals.count`` sizes the page loop.

Shape: STATELESS FULL RE-PULL. There is no usable incremental cursor (rows carry
DATEUPDT/change-date columns and the ES ``filters`` param accepts date ranges, but
the corpus is refreshed quarterly with revisions, so a stored watermark would
silently skip restated rows). Re-fetching the full corpus each refresh is correct;
the maintain step gates whether a given spec runs. Records are heterogeneous —
hundreds of distinct fields across endpoints and the same field name carries
different types per endpoint (e.g. ZIP is a string in /institutions but an int in
/financials) — so raw is written as streamed NDJSON (gzip), never schema'd parquet.
Largest specs (/sod ~283 pages, /financials ~168 pages at ~8s/page) stream
line-by-line to stay memory-bounded.
"""

import json

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
    raw_writer,
    load_state,
    save_state,
)

BASE_URL = "https://api.fdic.gov/banks"
PAGE_SIZE = 10000  # documented max limit per request

# Entity union — each id is also the API endpoint path segment.
ENTITY_IDS = [
    "demographics",
    "failures",
    "financials",
    "history",
    "institutions",
    "locations",
    "sod",
    "summary",
]

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
def _fetch_page(endpoint: str, offset: int) -> dict:
    """Fetch one page of an FDIC endpoint. Returns the parsed envelope."""
    resp = get(
        f"{BASE_URL}/{endpoint}",
        params={"limit": PAGE_SIZE, "offset": offset, "format": "json"},
        timeout=(10.0, 180.0),  # (connect, read) — financials pages are heavy
    )
    resp.raise_for_status()
    return resp.json()


def fetch_one(node_id: str) -> None:
    """Download one FDIC endpoint in full and write it as gzipped NDJSON.

    node_id is the spec id (e.g. "fdic-sod"); the entity/endpoint is recovered
    by stripping the "fdic-" prefix, and node_id itself is the asset name.
    """
    asset = node_id
    endpoint = node_id[len("fdic-"):]

    # Pin the total from the first response so the page loop has a fixed target
    # and doesn't drift while crawling.
    first = _fetch_page(endpoint, 0)
    total = int(first.get("totals", {}).get("count") or first.get("meta", {}).get("total") or 0)

    # Safety ceiling: detect a runaway loop / unexpected source growth. Sized off
    # the pinned total with generous headroom; firing means something is wrong.
    max_pages = (total // PAGE_SIZE) + 100

    written = 0
    page = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as f:
        envelope = first
        while True:
            rows = envelope.get("data") or []
            if not rows:
                break
            for row in rows:
                # Unwrap the ES {"data": {...}, "score": ...} envelope; the real
                # record is row["data"].
                record = row.get("data", row)
                f.write(json.dumps(record, separators=(",", ":")))
                f.write("\n")
                written += 1

            page += 1
            offset = page * PAGE_SIZE
            if offset >= total:
                break
            if page > max_pages:
                raise RuntimeError(
                    f"{asset}: exceeded max_pages={max_pages} (total={total}, "
                    f"written={written}); source larger than expected — investigate."
                )
            if page % 10 == 0:
                print(f"  {asset}: page {page}, {written:,}/{total:,} records", flush=True)

            envelope = _fetch_page(endpoint, offset)

    print(f"  {asset}: done — {written:,} records (reported total {total:,})", flush=True)

    # Observability only — NOT a watermark or terminal flag. Lets later runs diff
    # "fetched 2.8M last week, 4k today — something's off".
    state = load_state(asset)
    state["last_run_stats"] = {"records": written, "reported_total": total}
    save_state(asset, state)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"fdic-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]
