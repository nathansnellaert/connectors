"""BJS download — Socrata-style REST API at api.ojp.gov/bjsdataset/v1/.

Ten published BJS aggregate datasets across two groups: NCVS Select (4) and
NIBRS National Estimates (6). Each resource is a 4-4 Socrata id reachable as
`<id>.json`. Every field comes back as a JSON string, and the column set
differs entirely between resources (NCVS incident records vs NIBRS estimate
tables), so raw is written as NDJSON — no shared parquet schema is meaningful.

Fetch shape: stateless full re-pull. Each resource is a single bounded table
(well under 1M rows, total corpus < 1GB) with no incremental/`since` filter —
reports are revised annually, so we re-fetch the whole resource every run and
overwrite. The default Socrata row cap is 1000; we pass an explicit `$limit`
(comfortably above actual row counts) and raise if a response comes back at the
cap, which would mean the table outgrew our limit and is being truncated.
"""

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_ndjson

BASE = "https://api.ojp.gov/bjsdataset/v1/"

# Per-resource $limit. Sized to comfortably exceed actual row counts per the
# research handoff (NCVS victimization ~200k, NCVS population ~500k, NIBRS
# estimates ~50k). If a response returns exactly this many rows we treat it as
# truncated and raise.
LIMITS = {
    # NCVS — personal/household victimization (incident-level survey records)
    "gcuy-rt5g": 200000,
    "gkck-euys": 200000,
    # NCVS — population denominators
    "r4j4-fdwx": 500000,
    "ya4e-n9zp": 500000,
    # NIBRS National Estimates
    "iv7i-eah6": 50000,
    "kj7p-vx4s": 50000,
    "ms42-n765": 50000,
    "r32q-bdaw": 50000,
    "uy37-xgmh": 50000,
    "x3sz-eb6y": 50000,
}

ENTITY_IDS = list(LIMITS.keys())

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
def _fetch(resource_id: str, limit: int) -> list:
    url = f"{BASE}{resource_id}.json"
    resp = get(url, params={"$limit": limit}, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp.json()


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    resource_id = node_id[len("bjs-"):]
    limit = LIMITS[resource_id]

    rows = _fetch(resource_id, limit)
    if not isinstance(rows, list):
        raise TypeError(
            f"{resource_id}: expected a JSON array, got {type(rows).__name__}"
        )

    # Truncation guard: hitting the cap means the table outgrew our $limit and
    # the download is silently incomplete — surface it rather than ship a
    # partial corpus.
    if len(rows) >= limit:
        raise AssertionError(
            f"{resource_id}: returned {len(rows)} rows at $limit={limit} — "
            "likely truncated; raise the limit for this resource."
        )

    print(f"{resource_id}: fetched {len(rows)} rows", flush=True)
    save_raw_ndjson(rows, asset)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bjs-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]
