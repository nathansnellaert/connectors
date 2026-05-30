"""CBO download node — eval-projections datasets from the us-cbo GitHub org.

Mechanism: `github_us_cbo` (bulk_download, auth=none). The cbo.gov data portal
is behind DataDome bot protection (HTTP 403 to automation); the only verifiable
machine-readable surface is the us-cbo GitHub org. We fetch each dataset as the
complete CSV from raw.githubusercontent.com/us-cbo/eval-projections/main/<path>.

Fetch shape: stateless full re-pull (shape 1). The corpus is tiny (16 CSVs,
~5 MB total) and CBO republishes the whole repo a few times per year after each
Budget and Economic Outlook — there is no since/cursor parameter, so each refresh
re-fetches the full files. Freshness gating is the maintain step's job.

Raw format: each CSV has a distinct, file-specific column schema (a generic
catalog fetcher can't declare 16 schemas), so we store the downloaded CSV bytes
verbatim via save_raw_file(extension="csv"). Transform parses each file.

State is used only for per-entity TTL-bound skipped markers + last-run stats;
there is no watermark (full re-pull every run).
"""

import time

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_file, load_state, save_state

STATE_VERSION = 1

# Raw files served from this prefix (verified HTTP 200 + valid CSV during probe).
BASE_URL = "https://raw.githubusercontent.com/us-cbo/eval-projections/main/"

# The authoritative entity union — original repo-relative CSV paths, copied from
# data/sources/cbo/steps/768a628d227544a8a20009b710f35efd/entity_union.json
ENTITY_IDS = [
    "input_data/actual_GDP.csv",
    "input_data/actuals.csv",
    "input_data/baseline_changes.csv",
    "input_data/baselines.csv",
    "output_data/debt_actuals_pct_GDP.csv",
    "output_data/debt_projection_errors.csv",
    "output_data/debt_projection_errors_summary_stats.csv",
    "output_data/deficit_actuals_pct_GDP.csv",
    "output_data/deficit_projection_errors.csv",
    "output_data/deficit_projection_errors_summary_stats.csv",
    "output_data/outlay_actuals_pct_GDP.csv",
    "output_data/outlay_projection_errors.csv",
    "output_data/outlay_projection_errors_summary_stats.csv",
    "output_data/revenue_actuals_pct_GDP.csv",
    "output_data/revenue_projection_errors.csv",
    "output_data/revenue_projection_errors_summary_stats.csv",
]


def _spec_id(entity_id: str) -> str:
    return f"cbo-{entity_id.lower().replace('_', '-')}"


# spec id -> original repo path. The spec id is lowercased + hyphenated, so the
# original path (which has mixed case and underscores) can't be recovered from
# it directly — this map carries it back for URL construction.
SPEC_ID_TO_PATH = {_spec_id(p): p for p in ENTITY_IDS}

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
    wait=wait_exponential(min=2, max=60),
    reraise=True,
)
def _fetch(url: str) -> httpx.Response:
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    path = SPEC_ID_TO_PATH[node_id]  # KeyError here is a real bug — let it raise
    url = BASE_URL + path

    state = load_state(asset)
    if state.get("schema_version") != STATE_VERSION:
        state = {"schema_version": STATE_VERSION}

    # Expire any stale skipped marker so source recovery is automatic.
    skipped = state.get("skipped")
    if skipped and skipped.get("expires_at", 0) > int(time.time()):
        print(f"  {asset}: still within skip TTL ({skipped.get('reason')}), skipping", flush=True)
        return

    try:
        resp = _fetch(url)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        # Permanent 4xx (file removed/renamed upstream): record a TTL-bound
        # skipped marker and return cleanly so one bad entity doesn't fail others.
        if 400 <= code < 500 and code != 429:
            print(f"  {asset}: permanent HTTP {code} for {url}; writing skipped marker", flush=True)
            state["skipped"] = {
                "reason": f"HTTP {code} for {url}",
                "expires_at": int(time.time()) + 14 * 86400,
            }
            save_state(asset, state)
            return
        raise

    content = resp.content  # raw CSV bytes, stored verbatim
    save_raw_file(content, asset, extension="csv")

    # Clear any previous skip and record run stats for cheap week-over-week diffing.
    state.pop("skipped", None)
    n_lines = content.count(b"\n")
    state["last_run_stats"] = {
        "bytes": len(content),
        "lines": n_lines,
        "etag": resp.headers.get("etag"),
        "url": url,
    }
    save_state(asset, state)
    print(f"  {asset}: fetched {len(content)} bytes, {n_lines} lines", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id=_spec_id(eid), fn=fetch_one, kind="download")
    for eid in ENTITY_IDS
]
