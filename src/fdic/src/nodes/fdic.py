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

DEEP-PAGINATION CAP. The ES-backed API rejects ``offset >= 2_000_000`` with a
400 (verified by probing: offset 1_990_000 -> 200, offset 2_000_000 -> 400). So
plain offset paging can only reach the first 2,000,000 rows of any endpoint.
/sod has ~2.82M rows and overflows this window — it is fetched PARTITIONED by its
annual ``YEAR`` field (1994..present, ~76-90k rows/year, each well under the cap).
Every other endpoint is under 2M and uses plain offset paging; a guard raises a
clear error if any of them grows past the reachable window so the failure is
legible instead of a cryptic 400 (the fix at that point is to add the endpoint to
PARTITION_FIELD).

Shape: STATELESS FULL RE-PULL. There is no usable incremental cursor (rows carry
DATEUPDT/change-date columns and the ES ``filters`` param accepts date ranges, but
the corpus is refreshed quarterly with revisions, so a stored watermark would
silently skip restated rows). Re-fetching the full corpus each refresh is correct;
the maintain step gates whether a given spec runs. Records are heterogeneous —
hundreds of distinct fields across endpoints and the same field name carries
different types per endpoint (e.g. ZIP is a string in /institutions but an int in
/financials) — so raw is written as streamed NDJSON (gzip), never schema'd parquet.
Largest specs (/sod, /financials) stream line-by-line to stay memory-bounded.
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

# Offset paging is only valid while offset < this value (the ES max_result_window
# wall, verified by probe). An endpoint whose total exceeds this cannot be reached
# by plain offset paging and MUST be partitioned.
MAX_REACHABLE_ROWS = 2_000_000

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

# Endpoints that exceed MAX_REACHABLE_ROWS and must be crawled per-partition.
# Maps endpoint -> integer field to partition on. /sod is annual (Summary of
# Deposits); each YEAR is far under the offset cap.
PARTITION_FIELD = {
    "sod": "YEAR",
}

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
def _fetch_page(endpoint: str, offset: int, filters: str | None = None) -> dict:
    """Fetch one page of an FDIC endpoint. Returns the parsed envelope."""
    params = {"limit": PAGE_SIZE, "offset": offset, "format": "json"}
    if filters:
        params["filters"] = filters
    resp = get(
        f"{BASE_URL}/{endpoint}",
        params=params,
        timeout=(10.0, 180.0),  # (connect, read) — financials pages are heavy
    )
    resp.raise_for_status()
    return resp.json()


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _first_value(endpoint: str, field: str, order: str):
    """Return the value of ``field`` on the first row when sorted ASC/DESC.

    Used to discover the partition-value range without hardcoding years.
    """
    resp = get(
        f"{BASE_URL}/{endpoint}",
        params={"limit": 1, "offset": 0, "sort_by": field, "sort_order": order, "format": "json"},
        timeout=(10.0, 60.0),
    )
    resp.raise_for_status()
    rows = resp.json().get("data") or []
    if not rows:
        raise RuntimeError(f"{endpoint}: empty response while discovering {field} range")
    return rows[0].get("data", rows[0])[field]


def _discover_partitions(endpoint: str, field: str) -> list[int]:
    """Discover the inclusive integer range of ``field`` to iterate over."""
    lo = int(_first_value(endpoint, field, "ASC"))
    hi = int(_first_value(endpoint, field, "DESC"))
    if hi < lo:
        raise RuntimeError(f"{endpoint}: discovered {field} range {lo}..{hi} is empty")
    return list(range(lo, hi + 1))


def _crawl(endpoint: str, filters: str | None, writer, asset: str, first: dict | None = None) -> int:
    """Offset-page through ``endpoint`` (optionally filtered) into ``writer``.

    Returns the number of records written. Raises if the (filtered) total exceeds
    the offset-reachable window — that means the slice needs finer partitioning.
    """
    envelope = first if first is not None else _fetch_page(endpoint, 0, filters)
    total = int(envelope.get("totals", {}).get("count") or envelope.get("meta", {}).get("total") or 0)

    if total > MAX_REACHABLE_ROWS:
        raise RuntimeError(
            f"{asset}: slice (filters={filters!r}) has {total:,} rows, exceeding the "
            f"offset-reachable window {MAX_REACHABLE_ROWS:,}. Add finer partitioning "
            f"for endpoint '{endpoint}' in PARTITION_FIELD."
        )

    # Safety ceiling: detect a runaway loop. Generous headroom over the pinned total.
    max_pages = (total // PAGE_SIZE) + 100

    written = 0
    page = 0
    while True:
        rows = envelope.get("data") or []
        if not rows:
            break
        for row in rows:
            # Unwrap the ES {"data": {...}, "score": ...} envelope; real record is row["data"].
            record = row.get("data", row)
            writer.write(json.dumps(record, separators=(",", ":")))
            writer.write("\n")
            written += 1

        page += 1
        offset = page * PAGE_SIZE
        if offset >= total:
            break
        if page > max_pages:
            raise RuntimeError(
                f"{asset}: exceeded max_pages={max_pages} (total={total}, written={written}); "
                f"source larger than expected — investigate."
            )
        if page % 10 == 0:
            print(f"  {asset} ({filters or 'all'}): page {page}, {written:,}/{total:,}", flush=True)
        envelope = _fetch_page(endpoint, offset, filters)

    return written


def fetch_one(node_id: str) -> None:
    """Download one FDIC endpoint in full and write it as gzipped NDJSON.

    node_id is the spec id (e.g. "fdic-sod"); the endpoint is recovered by
    stripping the "fdic-" prefix, and node_id itself is the asset name.
    """
    asset = node_id
    endpoint = node_id[len("fdic-"):]
    field = PARTITION_FIELD.get(endpoint)

    written = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as f:
        if field:
            partitions = _discover_partitions(endpoint, field)
            print(f"  {asset}: partitioning by {field} over {partitions[0]}..{partitions[-1]} "
                  f"({len(partitions)} partitions)", flush=True)
            for part in partitions:
                w = _crawl(endpoint, f"{field}:{part}", f, asset)
                written += w
                print(f"  {asset}: {field}={part} -> {w:,} ({written:,} cumulative)", flush=True)
        else:
            written = _crawl(endpoint, None, f, asset)

    print(f"  {asset}: done — {written:,} records", flush=True)

    # Observability only — NOT a watermark or terminal flag. Lets later runs diff
    # "fetched 2.8M last week, 4k today — something's off".
    state = load_state(asset)
    state["last_run_stats"] = {"records": written}
    save_state(asset, state)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"fdic-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]
