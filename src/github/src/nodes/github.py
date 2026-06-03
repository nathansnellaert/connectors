"""GitHub Innovation Graph download node.

Source mechanism: innovation_graph_csv. GitHub publishes its only true
statistical-dataset surface as a set of long-format CSV metric files in the
github/innovationgraph repo's /data directory, refreshed quarterly. We fetch
each metric's CSV from the persistent raw.githubusercontent.com URL pinned to
the 'main' branch:

    https://raw.githubusercontent.com/github/innovationgraph/main/data/<metric>.csv

There are 8 metric files (one per entity in the union). Each is a full
long-format table (no pagination), small (tens of MB max), and re-published in
full each quarter with newly appended rows. There is NO incremental query
surface, so the correct shape is a stateless full re-pull every run: fetch the
whole CSV and overwrite. Quarterly revisions and late corrections are picked up
for free because we never trust a stored watermark.

Each metric has its own (stable) column set, so we parse each file with
pyarrow's CSV reader (per-file schema, read from the complete file in one pass)
and persist as parquet. Raw payloads are small enough to fit comfortably in RAM.
"""

import io
import time

import httpx
import pyarrow.csv as pacsv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_parquet, load_state, save_state

STATE_VERSION = 1

# Entity union (authoritative). For every entity the source filename is
# f"{entity_id}.csv" and the asset id is f"github-{entity_id.replace('_','-')}".
ENTITY_IDS = [
    "developers",
    "economy_collaborators",
    "git_pushes",
    "languages",
    "licenses",
    "organizations",
    "repositories",
    "topics",
]

_BASE_URL = "https://raw.githubusercontent.com/github/innovationgraph/main/data/"

# A permanent failure (e.g. a metric file renamed/removed upstream) parks the
# entity with a skipped marker for this long before we retry from scratch.
_SKIP_TTL_SECONDS = 14 * 86400

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
def _fetch_csv_bytes(url: str) -> bytes:
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.content


def _entity_from_node_id(node_id: str) -> str:
    # "github-economy-collaborators" -> "economy_collaborators"
    return node_id[len("github-"):].replace("-", "_")


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity = _entity_from_node_id(node_id)
    url = f"{_BASE_URL}{entity}.csv"

    state = load_state(asset)
    if state.get("schema_version") != STATE_VERSION:
        state = {}

    try:
        content = _fetch_csv_bytes(url)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        # Permanent (4xx other than 429): park this entity, don't raise out.
        if 400 <= code < 500 and code != 429:
            print(
                f"{asset}: permanent HTTP {code} for {url} - writing skipped marker",
                flush=True,
            )
            save_state(
                asset,
                {
                    "schema_version": STATE_VERSION,
                    "skipped": {
                        "reason": f"HTTP {code} at {url}",
                        "expires_at": int(time.time()) + _SKIP_TTL_SECONDS,
                    },
                },
            )
            return
        raise

    # Each metric file is a clean long-format table; read the complete file in
    # one pass so pyarrow infers a consistent per-column type.
    table = pacsv.read_csv(io.BytesIO(content))

    save_raw_parquet(table, asset)

    save_state(
        asset,
        {
            "schema_version": STATE_VERSION,
            "last_run_stats": {
                "records": table.num_rows,
                "bytes": len(content),
                "url": url,
            },
        },
    )


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"github-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]
