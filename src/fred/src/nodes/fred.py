"""FRED download — REST API v2 (https://api.stlouisfed.org/fred/v2/).

Mechanism: rest_v2 (chosen by research). v2 auth is an HTTP header
``Authorization: Bearer <api_key>`` — NOT the ``?api_key=`` query param that
v1 uses. The 32-char key comes from the FRED_API_KEY env var.

Three collect entities, three download specs:

  - ``fred-releases``     — the ~300 data releases. One small asset, full pull
                            every run (stateless; revisions picked up for free).
  - ``fred-series``       — series metadata for every series, fetched per release
                            via /release/series. Firehose: one NDJSON batch per
                            release (``fred-series-<release_id>``), streamed to
                            bound memory, soft per-run time budget, state tracks
                            which releases are done this cycle.
  - ``fred-observations`` — the (date, value) history for every series, fetched
                            per release via /release/observations (v2's bulk
                            endpoint — the whole reason v2 was chosen: ~300
                            release calls instead of ~840k per-series calls).
                            Same firehose shape as series.

Why per-release firehose for series/observations: the full corpus (~840k series,
their full histories) is far too large to re-pull into one file each run, so each
spec processes as many releases as fit in a time budget, writes one batch file per
release, and resumes from a watermark next run. ``releases`` is tiny so it stays a
plain stateless full pull.

Raw format: NDJSON throughout. The FRED v2 response schema could not be observed
during authoring (every /fred/v2/* path 401s without a key, and the docs host was
unreachable — see dev/fixtures/RECON.md), so field types are not verified here.
NDJSON is drift-tolerant; transform re-types on read. Envelope/pagination parsing
is deliberately defensive for the same reason. Route names below mirror the
well-known v1 surface and are confirmed by the first keyed (cloud) run.

Rate limit: 120 req/min documented. Each spec runs in its own process and they
share the api.stlouisfed.org host, so each process is capped well under the limit
(~32/min) to stay under 120 combined when specs run concurrently; 429s are caught
by the retry backoff as a safety net.
"""
import json
import os
import time

import httpx
from ratelimit import limits, sleep_and_retry
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
    raw_writer,
    delete_raw_file,
    load_state,
    save_state,
)

# Bump when the persisted firehose state contract changes (keys / watermark shape).
STATE_VERSION = 1

_BASE = "https://api.stlouisfed.org/fred/v2"

# Soft per-run wall-clock budget for the firehose specs. Hitting it returns
# cleanly with state advanced — the next run resumes. Not a hard cap.
MAX_FETCH_SECONDS = 1500  # 25 min

# Page sizes. v1 caps releases/series at 1000 and observations at 100000; v2 is
# assumed similar. Pagination terminates on a short page, so an unexpectedly
# small cap only costs extra requests, never correctness.
_PAGE_LIMIT_SERIES = 1000
_PAGE_LIMIT_OBS = 100000

# Runaway guard for the pagination loop (an API that ignores limit/offset would
# otherwise spin forever). Far above any real page count, so tripping it means
# the source grew past expectations or the loop misbehaved — surface it loudly.
_MAX_PAGES = 100_000

# ~32/min per process => <=96/min across the 3 specs, under the documented
# 120/min (~80% of the limit). Per-process; siblings hitting the same host do
# not coordinate, hence the conservative ceiling.
_RATE_CALLS = 32
_RATE_PERIOD = 60


# --- transport ---------------------------------------------------------------

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


def _require_key() -> str:
    key = os.environ.get("FRED_API_KEY")
    if not key:
        raise RuntimeError(
            "FRED_API_KEY env var is required — FRED v2 needs a free 32-char "
            "API key sent as 'Authorization: Bearer <key>'."
        )
    return key


@sleep_and_retry
@limits(calls=_RATE_CALLS, period=_RATE_PERIOD)
def _rate_limited_get(url: str, params: dict, headers: dict) -> httpx.Response:
    return get(url, params=params, headers=headers, timeout=(10.0, 120.0))


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _get_json(path: str, **params) -> dict | list:
    """GET a v2 endpoint as JSON. Retries transient failures; reraises the rest."""
    headers = {"Authorization": f"Bearer {_require_key()}"}
    full_params = {"file_type": "json", **params}
    resp = _rate_limited_get(_BASE + path, full_params, headers)
    resp.raise_for_status()  # inside the retry, so 429/5xx get retried
    return resp.json()


def _extract_records(payload, preferred_keys: tuple[str, ...]) -> list[dict]:
    """Pull the record list out of a v2 envelope without hardcoding its key.

    The exact envelope is unverified (see module docstring), so try the likely
    keys first, then fall back to the first list-of-dicts value present.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in preferred_keys:
            v = payload.get(k)
            if isinstance(v, list):
                return v
        for v in payload.values():
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                return v
    return []


def _paginate(path: str, base_params: dict, preferred_keys: tuple[str, ...], page_limit: int):
    """Yield records across offset-paginated pages until a short/empty page."""
    offset = 0
    pages = 0
    while True:
        payload = _get_json(path, **base_params, limit=page_limit, offset=offset)
        records = _extract_records(payload, preferred_keys)
        for rec in records:
            yield rec
        pages += 1
        if len(records) < page_limit:
            return
        if pages >= _MAX_PAGES:
            raise RuntimeError(
                f"{path} (params={base_params}) exceeded {_MAX_PAGES} pages at "
                f"offset {offset} — source grew past safety cap or pagination "
                f"is not honouring limit/offset."
            )
        offset += page_limit


# --- release enumeration -----------------------------------------------------

def _list_releases() -> list[dict]:
    rows = list(_paginate(
        "/releases",
        {"order_by": "release_id", "sort_order": "asc"},
        ("releases",),
        page_limit=_PAGE_LIMIT_SERIES,
    ))
    if not rows:
        raise RuntimeError(
            "/fred/v2/releases returned no records — wrong route, bad auth, or "
            "an unexpected response envelope."
        )
    return rows


def _release_id(rec: dict):
    rid = rec.get("id", rec.get("release_id"))
    if rid is None:
        raise KeyError(f"release record missing id field: {rec!r}")
    return rid


# --- specs -------------------------------------------------------------------

def fetch_releases(node_id: str) -> None:
    """fred-releases — full pull of the ~300 release catalog. Stateless."""
    asset = node_id
    rows = _list_releases()
    save_raw_ndjson(rows, asset)
    save_state(asset, {
        "schema_version": STATE_VERSION,
        "last_run_stats": {"records": len(rows)},
    })


def _firehose_by_release(
    node_id: str,
    subpath: str,
    preferred_keys: tuple[str, ...],
    page_limit: int,
) -> None:
    """Process releases one at a time, writing one NDJSON batch per release.

    State carries the set of release ids completed this cycle (the watermark).
    When every release is done, the cycle resets so the next run re-pulls from
    the top — picking up revised observations. Bounded by MAX_FETCH_SECONDS.
    """
    state = load_state(node_id)
    if state.get("schema_version") != STATE_VERSION:
        state = {"schema_version": STATE_VERSION}
    completed = set(state.get("completed", []))

    releases = _list_releases()
    all_ids = [_release_id(r) for r in releases]
    remaining = [rid for rid in all_ids if rid not in completed]
    if not remaining:
        print(f"{node_id}: all {len(all_ids)} releases done — starting new cycle", flush=True)
        completed = set()
        remaining = all_ids

    deadline = time.monotonic() + MAX_FETCH_SECONDS
    processed = 0
    records_this_run = 0

    for rid in remaining:
        if time.monotonic() > deadline:
            print(
                f"{node_id}: hit {MAX_FETCH_SECONDS}s budget after {processed} "
                f"releases this run — resuming next run", flush=True,
            )
            break

        asset = f"{node_id}-{rid}"  # batch key is pure release id, no slug
        try:
            n = _write_release_batch(asset, subpath, rid, preferred_keys, page_limit)
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code in (400, 404):
                # Permanent for this release only (e.g. release with no series /
                # observations). Mark done for this cycle and move on.
                print(f"{node_id}: release {rid} -> HTTP {code}, skipping", flush=True)
                completed.add(rid)
                save_state(node_id, {"schema_version": STATE_VERSION, "completed": sorted(completed)})
                continue
            raise  # transient already retried; other 4xx is a real problem

        # raw written first; then advance the watermark
        completed.add(rid)
        processed += 1
        records_this_run += n
        save_state(node_id, {
            "schema_version": STATE_VERSION,
            "completed": sorted(completed),
            "last_run_stats": {
                "releases_this_run": processed,
                "records_this_run": records_this_run,
                "releases_total": len(all_ids),
            },
        })
        if processed % 10 == 0:
            print(
                f"{node_id}: {processed} releases this run, "
                f"{records_this_run} records ({len(completed)}/{len(all_ids)} this cycle)",
                flush=True,
            )


def _write_release_batch(
    asset: str,
    subpath: str,
    rid,
    preferred_keys: tuple[str, ...],
    page_limit: int,
) -> int:
    """Stream one release's records to an NDJSON batch. Returns row count.

    Streamed (not accumulated) because a single release's observations can be
    very large. Empty batches are removed so no zero-row asset is left behind.
    """
    n = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip", encoding="utf-8") as fh:
        for row in _paginate(subpath, {"release_id": rid}, preferred_keys, page_limit):
            # Stamp the release id so downstream can join even if the bulk
            # envelope omits it per-row.
            row.setdefault("_release_id", rid)
            fh.write(json.dumps(row, separators=(",", ":")))
            fh.write("\n")
            n += 1
    if n == 0:
        delete_raw_file(asset, "ndjson.gz")
    return n


def fetch_series(node_id: str) -> None:
    """fred-series — series metadata per release (firehose)."""
    _firehose_by_release(node_id, "/release/series", ("seriess", "series"), _PAGE_LIMIT_SERIES)


def fetch_observations(node_id: str) -> None:
    """fred-observations — observation history per release, v2 bulk (firehose)."""
    _firehose_by_release(node_id, "/release/observations", ("observations",), _PAGE_LIMIT_OBS)


DOWNLOAD_SPECS = [
    NodeSpec(id="fred-releases", fn=fetch_releases, kind="download"),
    NodeSpec(id="fred-series", fn=fetch_series, kind="download"),
    NodeSpec(id="fred-observations", fn=fetch_observations, kind="download"),
]
