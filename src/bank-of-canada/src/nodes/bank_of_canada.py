"""Bank of Canada — Valet REST API download node.

Two entities (the rank-approved entity union):
  * series — the Valet series catalog (one row per series; label/description/link).
  * values — long-format observations across every series (series_id, date, value).

Valet exposes no bulk dump, but its observations endpoint accepts a
comma-separated list of series names and returns the full history of each in a
single response (rows joined by date). So `values` crawls in BATCHES of ~50
series — ~312 requests for the whole 15.5k-series corpus, instead of one
request per series (the prior attempt's 15.5k calls orphaned the step).

Incremental: a per-series date watermark lives in state. Series are sorted by
watermark so a batch shares a near-uniform `start_date`; refreshes pass it so
only new (plus a short revision overlap) rows come back. New series (no
watermark) cluster at the front and are fetched with full history.

Mechanism: valet_rest — no auth, persistent URLs. Docs:
https://www.bankofcanada.ca/valet/docs
"""

import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone

import httpx
import pyarrow as pa
from ratelimit import limits, sleep_and_retry
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from subsets_utils import (
    NodeSpec,
    get,
    load_state,
    save_state,
    raw_asset_exists,
    save_raw_parquet,
    raw_parquet_writer,
)

BASE = "https://www.bankofcanada.ca/valet"

# BoC revises recent observations; re-pull a week of overlap on each refresh.
# Duplicates produced by the overlap are dedup'd downstream by the transform's
# id-keyed merge on (series_id, obs_date).
OVERLAP_DAYS = 7

# Series per multi-series observations request. Probed live: 60 series -> HTTP
# 200 (URL ~1.3k chars); 200 series -> HTTP 302 (URL too long). 50 keeps a
# comfortable margin even with the longest (~48-char) series ids.
BATCH_SIZE = 50

SKIP_TTL = 14 * 86400          # permanent-failure marker lifetime (seconds)
FLUSH_ROWS = 200_000           # streamed-parquet flush threshold
MAX_CONSECUTIVE_BATCH_FAILS = 10   # systemic 4xx -> abort loudly, don't grind

_VALUES_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("obs_date", pa.string()),
    ("value", pa.string()),
])

_TRANSIENT_EXC = (
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
    httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError, httpx.ProxyError,
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
@sleep_and_retry
@limits(calls=4, period=1)   # ~a few rps — Valet publishes no hard limit; docs advise a gradual request rate
def _fetch_json(url, params=None):
    resp = get(url, params=params, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


def _list_series() -> dict:
    """Return the Valet series catalog as {series_id: {label, description, link}}."""
    data = _fetch_json(f"{BASE}/lists/series/json")
    series = data.get("series") or {}
    if not series:
        raise AssertionError("Valet /lists/series/json returned no series")
    return series


def _parse_obs(observations):
    """Flatten Valet observation rows into {series_id: [(date, value), ...]}.

    Each row is {"d": "YYYY-MM-DD", "<seriesId>": {"v": "value-as-string"}, ...}
    — one row may carry several series. Values arrive as strings; empty strings
    are stored as None. Keys are taken from the response itself (they echo the
    requested series ids)."""
    out: dict[str, list] = {}
    for row in observations:
        d = row.get("d")
        if not d:
            continue
        for key, cell in row.items():
            if key == "d":
                continue
            val = None
            if isinstance(cell, dict):
                raw = cell.get("v")
                if raw is not None and str(raw).strip() != "":
                    val = str(raw)
            out.setdefault(key, []).append((d, val))
    return out


def _batch_start_date(batch, watermarks):
    """start_date for a batch: None if any member lacks a watermark (needs full
    history), else the earliest member watermark minus the revision overlap."""
    wms = [watermarks.get(sid) for sid in batch]
    if any(w is None for w in wms):
        return None
    earliest = min(wms)
    return (date.fromisoformat(earliest) - timedelta(days=OVERLAP_DAYS)).isoformat()


def _fetch_obs(sids, start_date):
    """Fetch observations for one or more series. Returns {series_id: [(d, v)]}.
    Raises httpx.HTTPStatusError on HTTP failure (caller classifies)."""
    quoted = ",".join(urllib.parse.quote(s, safe=".") for s in sids)
    url = f"{BASE}/observations/{quoted}/json"
    params = {"start_date": start_date} if start_date else None
    data = _fetch_json(url, params=params)
    return _parse_obs(data.get("observations") or [])


def fetch_series(entity_id):
    """Download the full Valet series catalog into one parquet."""
    asset = "bank-of-canada-series"
    # Valet series catalog grows slowly as new series are published; recheck weekly.
    if raw_asset_exists(asset, max_age_days=7):
        print(f"  {asset}: fresh, skipping", flush=True)
        return

    series = _list_series()
    ids, labels, descs, links = [], [], [], []
    for sid, meta in sorted(series.items()):
        meta = meta or {}
        ids.append(sid)
        labels.append(meta.get("label"))
        descs.append(meta.get("description"))
        links.append(meta.get("link"))

    table = pa.table({
        "series_id": ids,
        "label": labels,
        "description": descs,
        "link": links,
    })
    save_raw_parquet(table, asset)
    print(f"  {asset}: {len(ids)} series", flush=True)


def fetch_values(entity_id):
    """Crawl observations for every Valet series in batches, incrementally.

    Per-series watermark in state; series sorted by watermark and chunked into
    multi-series requests. Refreshes pass start_date so only the recent window
    (plus overlap) returns. The delta for this run is streamed to one parquet —
    transform merges it by (series_id, obs_date)."""
    asset = "bank-of-canada-values"

    state = load_state(asset)
    if state.get("schema_version") != 1:
        if state:
            print(f"  {asset}: unknown schema_version, resetting state", flush=True)
        state = {}
    watermarks = dict(state.get("watermarks") or {})
    now = int(time.time())
    skipped = {
        k: v for k, v in (state.get("skipped") or {}).items()
        if v.get("expires_at", 0) > now
    }

    # Live catalog drives coverage; drop currently-skipped series, sort by
    # watermark ("" for new/unseen series sorts first), then chunk.
    series_ids = [s for s in _list_series().keys() if s not in skipped]
    series_ids.sort(key=lambda s: watermarks.get(s) or "")
    batches = [series_ids[i:i + BATCH_SIZE] for i in range(0, len(series_ids), BATCH_SIZE)]
    print(f"  {asset}: {len(series_ids)} series in {len(batches)} batches", flush=True)

    buf_sid, buf_date, buf_val = [], [], []
    total_rows = 0
    series_with_data = 0
    failed_series = 0
    consecutive_fails = 0

    with raw_parquet_writer(asset, _VALUES_SCHEMA) as writer:

        def _flush():
            if not buf_sid:
                return
            writer.write_batch(
                pa.record_batch([buf_sid, buf_date, buf_val], schema=_VALUES_SCHEMA)
            )
            buf_sid.clear()
            buf_date.clear()
            buf_val.clear()

        def _ingest(parsed):
            nonlocal total_rows, series_with_data
            for sid, pairs in parsed.items():
                if not pairs:
                    continue
                max_d = None
                for d, v in pairs:
                    buf_sid.append(sid)
                    buf_date.append(d)
                    buf_val.append(v)
                    if max_d is None or d > max_d:
                        max_d = d
                total_rows += len(pairs)
                series_with_data += 1
                existing = watermarks.get(sid)
                if max_d and (existing is None or max_d > existing):
                    watermarks[sid] = max_d

        for bi, batch in enumerate(batches):
            try:
                parsed = _fetch_obs(batch, _batch_start_date(batch, watermarks))
            except httpx.HTTPStatusError as e:
                code = e.response.status_code
                if code == 429 or 500 <= code < 600:
                    raise  # transient retries exhausted — fail the spec loudly
                # Permanent 4xx on the whole batch is unexpected (every id comes
                # from the live catalog). Isolate the bad series one at a time.
                consecutive_fails += 1
                if consecutive_fails > MAX_CONSECUTIVE_BATCH_FAILS:
                    raise RuntimeError(
                        f"{asset}: {consecutive_fails} consecutive batch failures "
                        f"(last HTTP {code}) — aborting, source likely broken"
                    )
                print(f"  {asset}: batch {bi} HTTP {code}, isolating per-series", flush=True)
                for sid in batch:
                    try:
                        _ingest(_fetch_obs([sid], _batch_start_date([sid], watermarks)))
                    except httpx.HTTPStatusError as e2:
                        c2 = e2.response.status_code
                        if c2 == 429 or 500 <= c2 < 600:
                            raise
                        print(f"  {asset}: series {sid} HTTP {c2}, skipping", flush=True)
                        skipped[sid] = {"reason": f"HTTP {c2}", "expires_at": now + SKIP_TTL}
                        failed_series += 1
            else:
                consecutive_fails = 0
                _ingest(parsed)

            if len(buf_sid) >= FLUSH_ROWS:
                _flush()
            if (bi + 1) % 50 == 0:
                print(f"  {asset}: batch {bi + 1}/{len(batches)}, {total_rows} rows", flush=True)

        _flush()

    # Raw is fully written and the writer is closed — only now persist state.
    save_state(asset, {
        "schema_version": 1,
        "watermarks": watermarks,
        "skipped": skipped,
        "last_run_stats": {
            "series_with_data": series_with_data,
            "series_failed": failed_series,
            "rows": total_rows,
            "ran_at": datetime.now(timezone.utc).isoformat(),
        },
    })
    print(
        f"  {asset}: done — {series_with_data} series with data, "
        f"{total_rows} rows, {failed_series} skipped",
        flush=True,
    )


DOWNLOAD_SPECS = [
    NodeSpec(id="bank-of-canada-series", fn=fetch_series, args=("series",), deps=(), kind="download"),
    NodeSpec(id="bank-of-canada-values", fn=fetch_values, args=("values",), deps=(), kind="download"),
]
