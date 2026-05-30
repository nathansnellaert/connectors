"""BJS download — Socrata-style REST API at api.ojp.gov/bjsdataset/v1/.

Ten published BJS aggregate datasets across two groups: NCVS Select (4) and
NIBRS National Estimates (6). Each resource is a 4-4 Socrata id reachable as
`<id>.json`. Every field comes back as a JSON string, and the column set
differs entirely between resources (NCVS person/household records vs NIBRS
estimate tables) — Socrata also omits null fields per row, so columns are
sparse within a resource. Raw is therefore written as NDJSON; no shared,
stable parquet schema is meaningful.

Fetch shape: stateless full re-pull with offset pagination. There is no
incremental/`since` filter (reports are revised annually), so each run pulls
the whole resource and overwrites. The Socrata default row cap is 1000, so we
paginate explicitly with `$limit`/`$offset` ordered by `:id` (stable row
ordering) until a short page terminates the loop. Several resources are large
(population denominators are 4-6M person-level rows; NIBRS victimization
counts ~3.7M), so we stream each page straight to a gzip-compressed NDJSON
file rather than buffering the full table in memory.

Probed row counts (2026-05-30): gcuy-rt5g 68.9k, gkck-euys 247.6k,
iv7i-eah6 6.98k, kj7p-vx4s 6.77k, ms42-n765 3.73M, r32q-bdaw 9.45k,
r4j4-fdwx 6.34M, uy37-xgmh 722k, x3sz-eb6y 10.8k, ya4e-n9zp 4.52M.
"""

import json
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
    raw_writer,
    load_state,
    save_state,
)

BASE = "https://api.ojp.gov/bjsdataset/v1/"

# Rows per request. Socrata honours large $limit values (probed up to 200k);
# 50k keeps each response a few MB and memory bounded while minimising round
# trips (the largest table, ~6.3M rows, is ~127 pages).
PAGE_SIZE = 50000

# Safety ceiling: detect runaway pagination / unexpected source growth. The
# biggest known table is ~127 pages; 2000 pages (~100M rows) is far above any
# real resource, so hitting it means something is wrong — raise, don't truncate.
MAX_PAGES = 2000

# Persistent Socrata resource ids — the entity union. Hardcoded per research
# (no machine-readable catalog exists; ids are stable across releases).
ENTITY_IDS = [
    "gcuy-rt5g",  # NCVS personal victimization (incident-level survey records)
    "gkck-euys",  # NCVS household victimization
    "iv7i-eah6",  # NIBRS property incidents
    "kj7p-vx4s",  # NIBRS property offenses
    "ms42-n765",  # NIBRS victimization counts/percentages
    "r32q-bdaw",  # NIBRS violent incidents
    "r4j4-fdwx",  # NCVS personal population (denominators)
    "uy37-xgmh",  # NIBRS victimization rates
    "x3sz-eb6y",  # NIBRS violent offenses
    "ya4e-n9zp",  # NCVS household population (denominators)
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
def _fetch_page(resource_id: str, offset: int) -> list:
    """Fetch one page of records, ordered by :id for stable offset paging."""
    url = f"{BASE}{resource_id}.json"
    resp = get(
        url,
        params={"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"},
        timeout=(15.0, 240.0),
    )
    resp.raise_for_status()
    rows = resp.json()
    if not isinstance(rows, list):
        raise TypeError(
            f"{resource_id}: expected a JSON array, got {type(rows).__name__}"
        )
    return rows


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_count(resource_id: str) -> int:
    """Authoritative row count via SoQL count(*) — used only as a sanity log."""
    url = f"{BASE}{resource_id}.json"
    resp = get(url, params={"$select": "count(*)"}, timeout=(15.0, 120.0))
    resp.raise_for_status()
    payload = resp.json()
    return int(payload[0]["count"])


def _mark_skipped(asset: str, resource_id: str, reason: str) -> None:
    """Record a TTL-bound skip for a permanently-failing resource."""
    state = load_state(asset)
    skipped = state.get("skipped", {})
    skipped[resource_id] = {
        "reason": reason,
        "expires_at": int(time.time()) + 14 * 86400,
    }
    state["skipped"] = skipped
    save_state(asset, state)


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    resource_id = node_id[len("bjs-"):]

    try:
        total = _fetch_count(resource_id)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code != 429 and 400 <= code < 500:
            # Permanent (e.g. 404 dataset.missing) — skip this resource, don't
            # fail the whole DAG. These ids are documented as persistent, so a
            # 4xx means the resource genuinely moved/retired.
            reason = f"HTTP {code} on count for {resource_id}"
            print(f"{resource_id}: permanent error, skipping — {reason}", flush=True)
            _mark_skipped(asset, resource_id, reason)
            return
        raise

    print(f"{resource_id}: count(*)={total:,}; paginating @ {PAGE_SIZE}", flush=True)

    written = 0
    offset = 0
    page_no = 0
    # Stream page-by-page to a gzip NDJSON file so multi-million-row tables stay
    # memory bounded. Raw is written before any state, always.
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as fh:
        while True:
            page_no += 1
            if page_no > MAX_PAGES:
                raise AssertionError(
                    f"{resource_id}: exceeded MAX_PAGES={MAX_PAGES} at "
                    f"offset={offset} (count(*)={total}) — likely runaway "
                    "pagination or unexpected source growth."
                )
            try:
                rows = _fetch_page(resource_id, offset)
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                if code != 429 and 400 <= code < 500:
                    raise AssertionError(
                        f"{resource_id}: HTTP {code} at offset={offset} "
                        f"after {written} rows written — partial download, "
                        "refusing to ship a truncated table."
                    ) from exc
                raise

            for row in rows:
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
            written += len(rows)

            if page_no % 10 == 0 or len(rows) < PAGE_SIZE:
                print(
                    f"{resource_id}: page {page_no}, {written:,}/{total:,} rows",
                    flush=True,
                )

            if len(rows) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    print(f"{resource_id}: done — {written:,} rows (count(*)={total:,})", flush=True)

    # State is informational only (stateless full re-pull): record run stats so
    # later runs can diff "fetched 6.3M last week, 4k today — something's off".
    save_state(asset, {
        "schema_version": 1,
        "last_run_stats": {"records": written, "count_reported": total},
    })


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bjs-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]
