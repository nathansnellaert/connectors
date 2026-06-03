"""FRED download — REST, mechanism rest_v2 for the bulk observations win.

Verified against the live API + docs at authoring time (the docs host bot-filters
plain GETs; read via a headless browser — see dev/RECON notes). Two facts drove
the design:

  1. FRED API **v2 exposes exactly ONE endpoint**: ``fred/v2/release/observations``
     — "get the observations for all series on a release, full history, in bulk."
     There is NO ``/fred/v2/releases`` and NO ``/fred/v2/release/series``. v2 is a
     bulk-observations surface, nothing else. Its auth is an HTTP header
     ``Authorization: Bearer <key>`` and it paginates with an opaque ``next_cursor``
     (``has_more``/``next_cursor``), NOT limit/offset.
  2. The release catalogue and per-series metadata therefore come from the long-
     standing **v1** endpoints (``/fred/releases``, ``/fred/release/series``), which
     authenticate with an ``&api_key=`` query param and paginate with limit/offset.
     The same free 32-char key works for both surfaces (FRED_API_KEY env var).

This is consistent with research's handoff: v2 is the chosen mechanism *for the
observations bulk win* (~300 release calls vs ~840k per-series calls); v1 supplies
the enumeration v2 structurally cannot. One key, two auth styles — do not mix them.

Three collect entities, three download specs:

  - ``fred-releases``     — the ~300-release catalogue. v1 ``/fred/releases``.
                            Tiny; stateless full pull every run (revisions free).
  - ``fred-series``       — series metadata for every series, per release via v1
                            ``/fred/release/series``. Firehose: one NDJSON batch
                            per release (``fred-series-<release_id>``), soft per-run
                            time budget, state tracks releases done this cycle.
  - ``fred-observations`` — full (date, value) history for every series, per release
                            via v2 ``/fred/v2/release/observations`` (the bulk
                            endpoint). Same per-release firehose shape; cursor
                            pagination within each release.

Why per-release firehose for series/observations: the corpus (~840k series and
their full histories) is far too large to re-pull into one file each run, so each
spec processes as many releases as fit in a wall-clock budget, writes one batch
file per release, and resumes from a watermark (the set of completed release ids)
next run. When every release is done the cycle resets and re-pulls from the top,
picking up revisions for free. ``releases`` is tiny so it stays a plain full pull.

Raw format: NDJSON throughout — the v2 series record carries a free-form ``notes``
blob and optional fields, so it is drift-prone; transform re-types on read.
Observations are flattened to one ``{series_id, date, value, release_id}`` row each
(value is FRED's numeric *string*, "." = missing — preserved verbatim).

Rate limit: 120 req/min documented, enforced server-side **per API key** — so it is
shared across the three specs (each runs in its own process against the same key).
Each process is capped well under a third of the budget; 429s are caught by the
retry backoff as a safety net.
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

_V1_BASE = "https://api.stlouisfed.org/fred"
_V2_BASE = "https://api.stlouisfed.org/fred/v2"

# Soft per-run wall-clock budget for the firehose specs. Hitting it returns
# cleanly with state advanced — the next run resumes. Deliberate pacing, not a cap.
MAX_FETCH_SECONDS = 1500  # 25 min

# v1 paginates with limit/offset. Releases/series cap at 1000 per page.
_V1_PAGE_LIMIT = 1000
# v2 observations: limit is observations-per-page, max 500000 (the documented
# ceiling and default). Big pages => few requests for the huge bulk responses.
_V2_OBS_LIMIT = 500000

# Runaway guard for any pagination loop (an API ignoring limit/offset or cursor
# would otherwise spin forever). Far above any real page count, so tripping it
# means the source grew past expectations or paging misbehaved — surface it loudly.
_MAX_PAGES = 200_000

# Documented 120/min is shared across the 3 specs (per-key, server-side). Cap each
# process at ~32/min => <=96/min combined, ~80% of the limit. Per-process limiter;
# siblings don't coordinate, hence the conservative ceiling.
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
            "FRED_API_KEY env var is required — FRED REST needs a free 32-char "
            "API key (https://fredaccount.stlouisfed.org/apikey). v1 sends it as "
            "the &api_key= query param; v2 sends it as 'Authorization: Bearer <key>'."
        )
    return key


@sleep_and_retry
@limits(calls=_RATE_CALLS, period=_RATE_PERIOD)
def _rate_limited_get(url: str, params: dict, headers: dict) -> httpx.Response:
    return get(url, params=params, headers=headers, timeout=(10.0, 180.0))


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _get_v1(path: str, **params) -> dict:
    """GET a v1 endpoint as JSON. Key goes in the api_key query param."""
    full = {"file_type": "json", "api_key": _require_key(), **params}
    resp = _rate_limited_get(_V1_BASE + path, full, {})
    resp.raise_for_status()  # inside the retry, so 429/5xx get retried
    return resp.json()


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _get_v2(path: str, **params) -> dict:
    """GET a v2 endpoint as JSON. Key goes in the Authorization: Bearer header."""
    headers = {"Authorization": f"Bearer {_require_key()}"}
    full = {"format": "json", **params}
    resp = _rate_limited_get(_V2_BASE + path, full, headers)
    resp.raise_for_status()
    return resp.json()


# --- v1 offset pagination ----------------------------------------------------

def _paginate_v1(path: str, base_params: dict, list_key: str):
    """Yield records from a limit/offset-paginated v1 endpoint.

    Termination: the v1 envelope echoes an authoritative ``count``; we stop once
    we've seen that many, or on a short page, whichever comes first. A page-count
    safety cap raises if neither fires (paging not honoured / unbounded growth).
    """
    offset = 0
    pages = 0
    yielded = 0
    while True:
        payload = _get_v1(path, **base_params, limit=_V1_PAGE_LIMIT, offset=offset)
        records = payload.get(list_key, [])
        if not isinstance(records, list):
            raise RuntimeError(
                f"{path}: expected list under '{list_key}', got "
                f"{type(records).__name__} (envelope keys={list(payload)[:8]})"
            )
        for rec in records:
            yield rec
        yielded += len(records)
        pages += 1

        count = payload.get("count")
        if isinstance(count, int) and yielded >= count:
            return
        if len(records) < _V1_PAGE_LIMIT:
            return
        if pages >= _MAX_PAGES:
            raise RuntimeError(
                f"{path} (params={base_params}) exceeded {_MAX_PAGES} pages — "
                f"source grew past safety cap or limit/offset not honoured."
            )
        offset += _V1_PAGE_LIMIT


def _list_releases() -> list[dict]:
    rows = list(_paginate_v1(
        "/releases", {"order_by": "release_id", "sort_order": "asc"}, "releases"
    ))
    if not rows:
        raise RuntimeError(
            "/fred/releases returned no records — wrong route, bad auth, or an "
            "unexpected response envelope."
        )
    return rows


def _release_ids() -> list[int]:
    ids = []
    for rec in _list_releases():
        rid = rec.get("id", rec.get("release_id"))
        if rid is None:
            raise KeyError(f"release record missing id field: {rec!r}")
        ids.append(rid)
    return ids


# --- per-release row iterators -----------------------------------------------

def _iter_series_rows(rid):
    """v1 series metadata for one release. Yields the raw series dicts, stamped
    with the originating release id so downstream can join."""
    for rec in _paginate_v1("/release/series", {"release_id": rid}, "seriess"):
        rec.setdefault("_release_id", rid)
        yield rec


def _iter_observation_rows(rid):
    """v2 bulk observations for one release. Yields flat
    {series_id, date, value, release_id} rows.

    Cursor pagination: each page carries ``series`` (each with embedded
    ``observations``), ``has_more`` and (while more) ``next_cursor``. A series can
    straddle a page boundary — its observations simply continue on the next page;
    flattening per page and concatenating into one batch file handles that for free.
    """
    cursor = None
    pages = 0
    prev_cursor = object()  # sentinel; never equal to a real cursor string
    while True:
        params = {"release_id": rid, "limit": _V2_OBS_LIMIT}
        if cursor is not None:
            params["next_cursor"] = cursor
        payload = _get_v2("/release/observations", **params)

        series_list = payload.get("series", [])
        if not isinstance(series_list, list):
            raise RuntimeError(
                f"/release/observations release {rid}: expected list under "
                f"'series', got {type(series_list).__name__} "
                f"(envelope keys={list(payload)[:8]})"
            )
        for s in series_list:
            sid = s.get("series_id")
            for obs in s.get("observations", []):
                yield {
                    "series_id": sid,
                    "date": obs.get("date"),
                    "value": obs.get("value"),
                    "release_id": rid,
                }

        pages += 1
        if not payload.get("has_more"):
            return
        cursor = payload.get("next_cursor")
        if not cursor:
            raise RuntimeError(
                f"/release/observations release {rid}: has_more=true but no "
                f"next_cursor — cannot continue pagination."
            )
        if cursor == prev_cursor:
            raise RuntimeError(
                f"/release/observations release {rid}: next_cursor did not advance "
                f"({cursor!r}) — server is repeating a page."
            )
        prev_cursor = cursor
        if pages >= _MAX_PAGES:
            raise RuntimeError(
                f"/release/observations release {rid} exceeded {_MAX_PAGES} pages "
                f"— source grew past safety cap or cursor not advancing."
            )


# --- specs -------------------------------------------------------------------

def fetch_releases(node_id: str) -> None:
    """fred-releases — full pull of the ~300-release catalogue (v1). Stateless."""
    asset = node_id
    rows = _list_releases()
    save_raw_ndjson(rows, asset)
    save_state(asset, {
        "schema_version": STATE_VERSION,
        "last_run_stats": {"records": len(rows)},
    })


def _firehose_by_release(node_id: str, row_iter) -> None:
    """Process releases one at a time, writing one NDJSON batch per release.

    ``row_iter`` is a callable ``rid -> iterator of dict rows``. State carries the
    set of release ids completed this cycle (the watermark). When every release is
    done the cycle resets so the next run re-pulls from the top. Bounded by
    MAX_FETCH_SECONDS — hitting it returns cleanly with state advanced.
    """
    state = load_state(node_id)
    if state.get("schema_version") != STATE_VERSION:
        state = {"schema_version": STATE_VERSION}
    completed = set(state.get("completed", []))

    all_ids = _release_ids()
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

        asset = f"{node_id}-{rid}"  # batch key is the pure release id, no slug
        try:
            n = _write_batch(asset, row_iter, rid)
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code in (400, 404):
                # Permanent for THIS release only (e.g. a release with no series).
                # Mark done for this cycle and move on — per-entity failure stays
                # per-entity; the firehose does not raise out for one bad release.
                print(f"{node_id}: release {rid} -> HTTP {code}, skipping", flush=True)
                completed.add(rid)
                save_state(node_id, {"schema_version": STATE_VERSION, "completed": sorted(completed)})
                continue
            raise  # transient already retried; other 4xx is a real problem

        # raw written first, then advance the watermark
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
                f"{node_id}: {processed} releases this run, {records_this_run} records "
                f"({len(completed)}/{len(all_ids)} this cycle)", flush=True,
            )


def _write_batch(asset: str, row_iter, rid) -> int:
    """Stream one release's rows to an NDJSON batch. Returns the row count.

    Streamed (not accumulated) because a single release's observations can be
    enormous. Empty batches are removed so no zero-row asset is left behind.
    """
    n = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip", encoding="utf-8") as fh:
        for row in row_iter(rid):
            fh.write(json.dumps(row, separators=(",", ":")))
            fh.write("\n")
            n += 1
    if n == 0:
        delete_raw_file(asset, "ndjson.gz")
    return n


def fetch_series(node_id: str) -> None:
    """fred-series — series metadata per release (v1 firehose)."""
    _firehose_by_release(node_id, _iter_series_rows)


def fetch_observations(node_id: str) -> None:
    """fred-observations — observation history per release (v2 bulk firehose)."""
    _firehose_by_release(node_id, _iter_observation_rows)


DOWNLOAD_SPECS = [
    NodeSpec(id="fred-releases", fn=fetch_releases, kind="download"),
    NodeSpec(id="fred-series", fn=fetch_series, kind="download"),
    NodeSpec(id="fred-observations", fn=fetch_observations, kind="download"),
]
