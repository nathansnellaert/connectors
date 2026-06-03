"""Federal Reserve — Data Download Program (DDP) download step.

Mechanism: `ddp` bulk_download. For each statistical release `rel` code we fetch
    https://www.federalreserve.gov/datadownload/Output.aspx?rel={REL}&filetype=zip
which returns a single ZIP per release containing the full release — every series,
full history — as SDMX-ML 1.0 compact-format XML (`{REL}_data.xml`) plus a
structure document (`{REL}_struct.xml`) and XSD schemas. Only the stable `rel`
code is needed; no series hash.

Fetch shape: stateless full re-pull (shape 1). Each release ZIP is a complete
snapshot (full history including any revisions), so we re-fetch the whole ZIP per
refresh and overwrite. The corpus is modest (H15, the largest, is ~67MB; most are
far smaller — PRATES ~54KB, H6 ~1.4MB), so a full re-pull is cheap. The incremental
`from`/`to`/`lastobs` params exist but are deliberately omitted to capture revisions
for free. Raw payloads are opaque ZIP bytes → `save_raw_file(..., extension="zip")`.
The downstream transform unzips and parses the SDMX-ML.
"""

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_file, load_state, save_state

# Entity union — the ~17 DDP statistical releases (copied from entity_union.json).
ENTITY_IDS = [
    "CHGDEL",
    "CP",
    "DSR",
    "E2",
    "G17",
    "G19",
    "G20",
    "H10",
    "H15",
    "H3",
    "H41",
    "H6",
    "H8",
    "PRATES",
    "SCOOS",
    "SLOOS",
    "Z1",
]

STATE_VERSION = 1
_SKIP_TTL_SECONDS = 14 * 86400  # 2 weeks before we retry a release that 4xx'd

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
def _fetch_zip(rel: str) -> bytes:
    """Download one release ZIP. Raises on non-2xx (permanent 4xx handled by caller,
    transient 5xx/429 retried by the decorator)."""
    url = "https://www.federalreserve.gov/datadownload/Output.aspx"
    resp = get(
        url,
        params={"rel": rel, "filetype": "zip"},
        timeout=(10.0, 300.0),  # H15 is ~67MB; allow a generous read timeout
    )
    resp.raise_for_status()
    return resp.content


def _entity_id(node_id: str) -> str:
    """Recover the upstream release code from a spec id (inverse of the id rule)."""
    rel = node_id[len("federal-reserve-"):]
    return rel.upper().replace("-", "_")


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    rel = _entity_id(node_id)

    try:
        content = _fetch_zip(rel)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        # Transient codes never reach here (retried + reraised by the decorator).
        # A permanent 4xx on a stable rel code is unexpected — record a TTL-bound
        # skip for THIS release and return cleanly so siblings keep going.
        if 400 <= code < 500 and code != 429:
            print(
                f"[federal-reserve] permanent HTTP {code} for rel={rel} "
                f"({exc.request.url}); writing skip marker",
                flush=True,
            )
            _record_skip(asset, rel, f"http_{code}")
            return
        raise

    # The ZIP must start with the PK local-file-header magic; anything else means
    # the endpoint returned an HTML error page with a 200 (defensive).
    if content[:2] != b"PK":
        print(
            f"[federal-reserve] rel={rel} returned {len(content)} bytes that are "
            f"not a ZIP (magic={content[:4]!r}); writing skip marker",
            flush=True,
        )
        _record_skip(asset, rel, "not_a_zip")
        return

    save_raw_file(content, asset, extension="zip")

    state = load_state(asset)
    state["schema_version"] = STATE_VERSION
    state.pop("skipped", None)  # successful fetch clears any prior skip
    state["last_run_stats"] = {"bytes": len(content), "rel": rel}
    save_state(asset, state)
    print(f"[federal-reserve] rel={rel}: saved {len(content)} bytes", flush=True)


def _record_skip(asset: str, rel: str, reason: str) -> None:
    import time

    state = load_state(asset)
    state["schema_version"] = STATE_VERSION
    skipped = state.get("skipped", {})
    skipped[rel] = {"reason": reason, "expires_at": int(time.time()) + _SKIP_TTL_SECONDS}
    state["skipped"] = skipped
    save_state(asset, state)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"federal-reserve-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]
