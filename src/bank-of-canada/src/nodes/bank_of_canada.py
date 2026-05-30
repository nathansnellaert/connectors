"""Bank of Canada Valet API — download step.

Mechanism: valet_rest (https://www.bankofcanada.ca/valet/). No auth, no key.

Two collect entities → two download specs:

  * ``series`` — the full series catalog from ``/lists/series/json``
    (one row per series: series_id, label, description, link). Small, stable,
    tabular → single parquet asset.

  * ``values`` — the long-format observation firehose across *all* ~15.6k
    series. Far too large for one file, so this is a batched firehose
    (shape 3): the series universe is sorted and walked in chunks, each chunk
    fetched in one comma-separated multi-series observation call and written as
    its own NDJSON batch asset ``bank-of-canada-values-{chunk_index}``.

    NDJSON (not parquet) is deliberate: the observation payload is
    heterogeneous. Most series carry a date dimension (``{"d": "...",
    "<sid>": {"v": "..."}}``) but some (e.g. ``AUC_BOND_*``) carry a
    non-date dimension keyed by ``bond_id`` etc. The dimension key is named by
    ``seriesDetail[sid].dimension.key`` and varies across series, so rows are
    parsed generically into ``{series_id, dim_key, dim_value, value}`` and the
    string-typed value is left for transform to coerce.

Refresh strategy for ``values``: full re-pull every run (overwriting each
batch). BoC series are revised, so trusting a per-series date watermark would
silently drop corrections; a full pass over the corpus is cheap (~300 chunked
calls, a few minutes). State is used only for *in-run crash resume* (which
chunk to continue from), never as a terminal flag — a completed pass resets to
chunk 0 on the next invocation. ``start_date`` incremental query is supported
by the API but intentionally unused for the reason above.

Freshness gating (whether a spec runs at all) is the maintain step's job.
"""

from __future__ import annotations

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
STATE_VERSION = 1

# Series per multi-series observation call. 50 keeps URLs ~1KB and per-chunk
# responses a few MB; ~313 chunks for the current ~15.6k-series corpus.
CHUNK_SIZE = 50

# A partial pass older than this is meaningless to resume (the corpus moves on);
# treat stale in-flight state as empty and start a fresh full pass.
STALE_RESUME_SECONDS = 2 * 86400

# Skipped-chunk markers (permanent 4xx on a chunk) expire so source recovery is
# automatic.
SKIP_TTL_SECONDS = 14 * 86400

# Safety ceiling: if the series listing balloons far past observed scale,
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
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


@sleep_and_retry
@limits(calls=4, period=1)  # polite ~4 rps; docs advise a gradual request rate
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _get_json(url: str, params: dict | None = None) -> dict:
    resp = get(url, params=params, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp.json()


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _fetch_series_index() -> list[str]:
    """Return the sorted list of all series ids exposed by the Valet API."""
    payload = _get_json(f"{BASE_URL}/lists/series/json")
    series = payload.get("series", {})
    if not series:
        raise AssertionError("lists/series returned no series — unexpected")
    if len(series) > MAX_SERIES:
        raise AssertionError(
            f"series count {len(series)} exceeds safety ceiling {MAX_SERIES}; "
            "investigate before crawling"
        )
    return sorted(series.keys())


# --------------------------------------------------------------------------- #
# Entity: series — the series catalog
# --------------------------------------------------------------------------- #

def fetch_series(node_id: str) -> None:
    asset = node_id  # the spec id IS the asset name
    payload = _get_json(f"{BASE_URL}/lists/series/json")
    series = payload.get("series", {})
    if not series:
        raise AssertionError("lists/series returned no series — unexpected")

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


def _purge_expired_skips(skipped: dict) -> dict:
    now = int(time.time())
    return {
        k: v
        for k, v in skipped.items()
        if not (isinstance(v, dict) and v.get("expires_at", 0) < now)
    }


def fetch_values(node_id: str) -> None:
    state_key = node_id  # "bank-of-canada-values"

    series_ids = _fetch_series_index()
    total_chunks = math.ceil(len(series_ids) / CHUNK_SIZE)

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
        state.get("series_count") != len(series_ids)
        or done >= total_chunks
        or stale
    )
    start = 0 if fresh else done
    skipped = _purge_expired_skips(state.get("skipped", {}) if not fresh else {})

    print(
        f"[{state_key}] {len(series_ids)} series, {total_chunks} chunks; "
        f"{'fresh pass' if fresh else f'resuming at chunk {start}'}",
        flush=True,
    )

    total_rows = 0
    for ci in range(start, total_chunks):
        batch_key = f"{ci:04d}"
        asset = f"{state_key}-{batch_key}"
        chunk = series_ids[ci * CHUNK_SIZE : (ci + 1) * CHUNK_SIZE]
        url = f"{BASE_URL}/observations/{','.join(chunk)}/json"
        try:
            payload = _get_json(url)
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            # Transient (429/5xx) is retried by the decorator; reaching here on
            # a 4xx means a permanent failure for this chunk.
            if code != 429 and 400 <= code < 500:
                print(
                    f"[{asset}] permanent {code} for chunk {ci} "
                    f"(url={url}); skipping",
                    flush=True,
                )
                skipped[batch_key] = {
                    "reason": f"http_{code}",
                    "expires_at": int(time.time()) + SKIP_TTL_SECONDS,
                }
                # raw-before-state holds: no raw written, advance progress only.
                save_state(state_key, {
                    "schema_version": STATE_VERSION,
                    "series_count": len(series_ids),
                    "total_chunks": total_chunks,
                    "completed_chunks": ci + 1,
                    "last_success_at": _now_iso(),
                    "skipped": skipped,
                })
                continue
            raise

        rows = _parse_observations(payload)
        # Write raw FIRST, then advance state.
        save_raw_ndjson(rows, asset)
        total_rows += len(rows)
        save_state(state_key, {
            "schema_version": STATE_VERSION,
            "series_count": len(series_ids),
            "total_chunks": total_chunks,
            "completed_chunks": ci + 1,
            "last_success_at": _now_iso(),
            "skipped": skipped,
        })

        if (ci + 1) % 25 == 0 or ci + 1 == total_chunks:
            print(
                f"[{state_key}] chunk {ci + 1}/{total_chunks} "
                f"(+{len(rows)} rows, {total_rows} this run)",
                flush=True,
            )

    # Stamp run stats for cross-run drift diagnostics.
    final = load_state(state_key)
    final["last_run_stats"] = {
        "chunks_fetched": total_chunks - start,
        "rows_this_run": total_rows,
        "series_count": len(series_ids),
        "skipped_chunks": len(skipped),
    }
    save_state(state_key, final)
    print(
        f"[{state_key}] pass complete: {total_rows} rows across "
        f"{total_chunks - start} chunks ({len(skipped)} skipped)",
        flush=True,
    )


DOWNLOAD_SPECS: list[NodeSpec] = [
    NodeSpec(id="bank-of-canada-series", fn=fetch_series, kind="download"),
    NodeSpec(id="bank-of-canada-values", fn=fetch_values, kind="download"),
]
