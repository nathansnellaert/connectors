"""Bank of Canada Valet API — download step.

Mechanism: valet_rest (https://www.bankofcanada.ca/valet/). No auth, no key.

Two collect entities -> two download specs:

  * ``series`` — the full series catalog from ``/lists/series/json``
    (one row per series: series_id, label, description, link). ~15.6k rows,
    stable, tabular -> single parquet asset.

  * ``values`` — the long-format observation firehose across *all* ~15.6k
    series. Far too large for one file, so this is a batched firehose
    (shape 3): the series universe is sorted and walked in chunks of
    ``CHUNK_SIZE``, each chunk fetched in one comma-separated multi-series
    ``/observations`` call and written as its own NDJSON batch asset
    ``bank-of-canada-values-NNNN``.

    NDJSON (not parquet) is deliberate: the observation payload is
    heterogeneous. Most series carry a date dimension (``{"d": "...",
    "<sid>": {"v": "..."}}``) but some carry a non-date dimension keyed by
    ``bond_id`` etc. The dimension key is named by
    ``seriesDetail[sid].dimension.key`` and varies across series, so rows are
    parsed generically into ``{series_id, dim_key, dim_value, value}`` and the
    string-typed value is left for transform to coerce.

Refresh strategy for ``values``: full re-pull every run. BoC series are
revised, so a per-series date watermark would silently drop corrections; a
full pass over the corpus is cheap (~157 chunked calls, ~2-3 min). State is
used only for *in-run crash resume* and *wall-clock pacing* — a completed pass
resets to chunk 0 on the next invocation; it is never a terminal flag.

Pacing: each invocation stops cleanly after ``MAX_FETCH_SECONDS`` with state
advanced, so the fetch can never be hard-killed mid-pass by a materialize
timeout (the failure mode of the prior attempt, which crawled 313 chunks with
no wall-clock guard and was orphaned). The full corpus normally completes well
inside the budget; if it ever doesn't, the next refresh resumes where this one
stopped.

Robustness: a single invalid id 404s an entire multi-series call, so a
permanent 4xx on a chunk falls back to fetching that chunk's series one at a
time, skipping only the genuinely-bad ids rather than losing the whole chunk.

Freshness gating (whether a spec runs at all) is the maintain step's job.
"""

from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone

import httpx
import pyarrow as pa
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
    save_raw_parquet,
    load_state,
    save_state,
)

BASE_URL = "https://www.bankofcanada.ca/valet"

# Bump when the `values` resume-state contract changes (keys / cursor shape).
STATE_VERSION = 2

# Series per multi-series observation call. 100 keeps the corpus to ~157 calls
# (fast) while bounding the heaviest response to ~9MB / ~55MB peak RSS (probed).
CHUNK_SIZE = 100

# Soft wall-clock budget per invocation. The full corpus is ~2-3 min; this
# leaves generous headroom and guarantees a clean self-stop (state advanced)
# before any plausible materialize hard-kill. Hitting it is deliberate pacing,
# NOT an error — the next refresh resumes from the saved watermark.
MAX_FETCH_SECONDS = 300.0

# A partial pass older than this is meaningless to resume (the corpus moves on);
# treat stale in-flight state as empty and start a fresh full pass.
STALE_RESUME_SECONDS = 2 * 86400

# Skipped-series markers (permanent 4xx) expire so source recovery is automatic.
SKIP_TTL_SECONDS = 14 * 86400

# Safety ceiling: if the series listing balloons far past observed scale (~15.6k),
# surface it loudly rather than silently churning through tens of thousands of
# extra calls.
MAX_SERIES = 60_000

SERIES_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("label", pa.string()),
    ("description", pa.string()),
    ("link", pa.string()),
])

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
    # The Valet host intermittently returns a 200 with an empty / CRLF-only
    # body under load (observed failure: "Expecting value: line 2 column 1").
    # Treat a decode failure as a transient glitch worth a few retries; the
    # chunk fetcher keeps a per-series fallback as the deterministic backstop.
    if isinstance(exc, json.JSONDecodeError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


@sleep_and_retry
@limits(calls=5, period=1)  # polite ~5 rps; docs advise a gradual request rate
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(5),
    # Tight cap: one flaky chunk can't eat the whole wall-clock budget.
    wait=wait_exponential(min=2, max=30),
    reraise=True,
)
def _get_json(url: str, params: dict | None = None) -> dict:
    resp = get(url, params=params, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp.json()


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _list_series() -> dict:
    """Return the raw ``series`` mapping from /lists/series/json."""
    payload = _get_json(f"{BASE_URL}/lists/series/json")
    series = payload.get("series", {})
    if not series:
        raise AssertionError("lists/series returned no series — unexpected")
    if len(series) > MAX_SERIES:
        raise AssertionError(
            f"series count {len(series)} exceeds safety ceiling {MAX_SERIES}; "
            "investigate before crawling"
        )
    return series


# --------------------------------------------------------------------------- #
# Entity: series — the series catalog
# --------------------------------------------------------------------------- #

def fetch_series(node_id: str) -> None:
    asset = node_id  # the spec id IS the asset name
    series = _list_series()
    rows = [
        {
            "series_id": sid,
            "label": meta.get("label"),
            "description": meta.get("description"),
            "link": meta.get("link"),
        }
        for sid, meta in sorted(series.items())
    ]
    table = pa.Table.from_pylist(rows, schema=SERIES_SCHEMA)
    save_raw_parquet(table, asset)
    print(f"[{asset}] wrote {len(rows)} series", flush=True)


# --------------------------------------------------------------------------- #
# Entity: values — the observation firehose (batched, full re-pull)
# --------------------------------------------------------------------------- #

def _parse_observations(payload: dict) -> list[dict]:
    """Flatten a (multi-)series observation payload into long-format rows.

    Each observation row carries exactly one dimension key (the key NOT present
    in ``seriesDetail`` — usually ``d`` for a date, but e.g. ``bond_id`` for
    auction series) plus one entry per series id present on that row, each a
    ``{"v": "..."}`` dict. Values are strings (or absent); transform coerces.
    """
    detail = payload.get("seriesDetail", {})
    out: list[dict] = []
    for row in payload.get("observations", []):
        dim_keys = [k for k in row if k not in detail]
        if not dim_keys:
            continue
        dim_key = dim_keys[0]
        dim_value = row.get(dim_key)
        for sid, cell in row.items():
            if sid == dim_key or sid not in detail:
                continue
            value = cell.get("v") if isinstance(cell, dict) else None
            out.append(
                {
                    "series_id": sid,
                    "dim_key": dim_key,
                    "dim_value": dim_value,
                    "value": value,
                }
            )
    return out


def _is_permanent_4xx(exc: httpx.HTTPStatusError) -> bool:
    code = exc.response.status_code
    return code != 429 and 400 <= code < 500


def _fetch_chunk_rows(chunk: list[str], asset: str, skipped: dict) -> list[dict]:
    """Fetch one chunk of series, returning flattened long-format rows.

    Fast path: a single multi-series call. Two ways the bulk call can fail
    deterministically after retries are exhausted — a permanent 4xx (one
    invalid id 404s the whole call) or a persistent bad body (decode failure
    that didn't clear on retry). In both cases we fall back to per-series calls
    and skip only the genuinely-bad ids (TTL-bound marker) rather than losing
    the whole chunk to one bad member.
    """
    url = f"{BASE_URL}/observations/{','.join(chunk)}/json"
    try:
        return _parse_observations(_get_json(url))
    except httpx.HTTPStatusError as exc:
        if not _is_permanent_4xx(exc):
            raise
        reason = f"http_{exc.response.status_code}"
    except json.JSONDecodeError:
        # Retries (see _is_transient) didn't clear it — drop to per-series so a
        # single bad member can't sink the chunk.
        reason = "bad_json"
    print(
        f"[{asset}] bulk call failed ({reason}); falling back to per-series for "
        f"{len(chunk)} ids (url={url})",
        flush=True,
    )

    rows: list[dict] = []
    for sid in chunk:
        try:
            rows.extend(
                _parse_observations(_get_json(f"{BASE_URL}/observations/{sid}/json"))
            )
        except httpx.HTTPStatusError as exc:
            if not _is_permanent_4xx(exc):
                raise
            print(
                f"[{asset}] permanent {exc.response.status_code} for series "
                f"{sid}; skipping",
                flush=True,
            )
            skipped[sid] = {
                "reason": f"http_{exc.response.status_code}",
                "expires_at": int(time.time()) + SKIP_TTL_SECONDS,
            }
        except json.JSONDecodeError:
            # Persistent bad body for one series — skip it for this pass with a
            # short TTL (likely transient; a full re-pull retries it next run).
            print(
                f"[{asset}] persistent bad JSON for series {sid}; skipping",
                flush=True,
            )
            skipped[sid] = {
                "reason": "bad_json",
                "expires_at": int(time.time()) + 86400,
            }
    return rows


def _purge_expired_skips(skipped: dict) -> dict:
    now = int(time.time())
    return {
        k: v
        for k, v in skipped.items()
        if not (isinstance(v, dict) and v.get("expires_at", 0) < now)
    }


def _save_progress(state_key, series_count, total_chunks, completed, skipped):
    save_state(state_key, {
        "schema_version": STATE_VERSION,
        "series_count": series_count,
        "total_chunks": total_chunks,
        "completed_chunks": completed,
        "last_success_at": _now_iso(),
        "skipped": skipped,
    })


def fetch_values(node_id: str) -> None:
    state_key = node_id  # "bank-of-canada-values"
    started = time.monotonic()

    series_ids = sorted(_list_series().keys())
    series_count = len(series_ids)
    total_chunks = math.ceil(series_count / CHUNK_SIZE)

    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        if state:
            print(
                f"[{state_key}] state schema_version mismatch "
                f"({state.get('schema_version')} != {STATE_VERSION}); resetting",
                flush=True,
            )
        state = {}

    done = int(state.get("completed_chunks", 0))
    last_success = state.get("last_success_at")
    stale = True
    if last_success:
        try:
            age = (
                datetime.now(tz=timezone.utc)
                - datetime.fromisoformat(last_success)
            ).total_seconds()
            stale = age > STALE_RESUME_SECONDS
        except ValueError:
            stale = True

    # Resume an in-flight pass only if it matches the current corpus, isn't
    # already complete, and is recent. Otherwise start a fresh full re-pull.
    fresh = (
        state.get("series_count") != series_count
        or done >= total_chunks
        or stale
    )
    start = 0 if fresh else done
    skipped = _purge_expired_skips({} if fresh else state.get("skipped", {}))

    print(
        f"[{state_key}] {series_count} series, {total_chunks} chunks "
        f"(size {CHUNK_SIZE}); {'fresh pass' if fresh else f'resuming at chunk {start}'}",
        flush=True,
    )

    total_rows = 0
    last_done = start
    for ci in range(start, total_chunks):
        elapsed = time.monotonic() - started
        if elapsed > MAX_FETCH_SECONDS:
            print(
                f"[{state_key}] wall-clock budget reached at chunk {ci}/"
                f"{total_chunks} ({elapsed:.0f}s); stopping cleanly, will resume",
                flush=True,
            )
            break

        asset = f"{state_key}-{ci:04d}"
        chunk = series_ids[ci * CHUNK_SIZE : (ci + 1) * CHUNK_SIZE]
        rows = _fetch_chunk_rows(chunk, asset, skipped)

        # Write raw FIRST, then advance state.
        save_raw_ndjson(rows, asset)
        total_rows += len(rows)
        last_done = ci + 1
        _save_progress(state_key, series_count, total_chunks, last_done, skipped)

        if last_done % 25 == 0 or last_done == total_chunks:
            print(
                f"[{state_key}] chunk {last_done}/{total_chunks} "
                f"(+{len(rows)} rows, {total_rows} this run, {elapsed:.0f}s)",
                flush=True,
            )

    # Stamp run stats for cross-run drift diagnostics.
    final = load_state(state_key)
    final["last_run_stats"] = {
        "chunks_done_this_run": last_done - start,
        "completed_chunks": last_done,
        "total_chunks": total_chunks,
        "rows_this_run": total_rows,
        "series_count": series_count,
        "skipped_series": len(skipped),
        "seconds": round(time.monotonic() - started, 1),
    }
    save_state(state_key, final)
    print(
        f"[{state_key}] run done: {total_rows} rows across {last_done - start} "
        f"chunks ({last_done}/{total_chunks} complete, {len(skipped)} skipped)",
        flush=True,
    )


DOWNLOAD_SPECS: list[NodeSpec] = [
    NodeSpec(id="bank-of-canada-series", fn=fetch_series, kind="download"),
    NodeSpec(id="bank-of-canada-values", fn=fetch_values, kind="download"),
]
