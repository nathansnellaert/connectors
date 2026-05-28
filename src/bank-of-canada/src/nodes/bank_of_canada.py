"""Bank of Canada — Valet REST download nodes.

Two entities (the rank-approved union):

  * ``series`` — the full Valet series catalog (one row per series: id, label,
    description, link). A snapshot, overwritten each run.
  * ``values`` — long-format observations across every series
    (one row per (series_id, date, value)). A bounded firehose: ~15.5k series,
    each ``/observations/{names}/json`` call returning the full history of the
    requested series, joined by date. Backfilled across runs via a sorted
    series-id cursor (completeness derived from cursor vs the catalog — no
    terminal flag), then refreshed incrementally via ``start_date``.

Mechanism: ``valet_rest`` — no auth, no key.

Why the listing gets a long read timeout: ``/lists/series/json`` is a single
~3.5 MB document that the server occasionally takes >120s to emit (observed: a
clean 120s read-timeout on one probe, 0.15s on the next). Both nodes fetch it
first, so a tight timeout there hangs the whole run — the prior attempt burned
61 minutes on it and was cancelled before writing anything. A generous read
timeout plus transient-retry absorbs the slow-but-completing case.
"""

import re
import time
from datetime import date, datetime, timedelta, timezone

import httpx
import pyarrow as pa
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from subsets_utils import NodeSpec, get, load_state, save_raw_parquet, save_state

BASE = "https://www.bankofcanada.ca/valet"

# Watermark/cursor contract version. Bump when the `values` state shape changes
# so stale state from an older shape is discarded rather than misread.
STATE_VERSION = 1

# Series per multi-series observation request. 50 ids → ~1 KB URL (proven:
# 1057 chars), ~2.4 MB response, ~25k long rows, ~1s. Small enough that a
# batch 404 (one discontinued id 404s the whole batch) only forces a cheap
# 50-call per-series fallback.
REQUEST_BATCH = 50

# Flush a backfill parquet every N consumed chunks. Bounds buffer memory
# (~N*25k rows) and how stale the resume cursor can get. ~312 chunks total →
# ~21 files for a full backfill.
FLUSH_EVERY_CHUNKS = 15

# Soft per-run budget for the firehose. A full backfill is ~6 min, so this is
# a safety ceiling, not a normal stopping point — hitting it returns cleanly
# with the cursor advanced and the next run resumes.
MAX_FETCH_SECONDS = 3000

# Refresh re-fetch window: re-pull recent observations to absorb late
# revisions. Duplicates from the overlap are dedup'd downstream by the
# (series_id, date) merge.
OVERLAP_DAYS = 14

# The big catalog listing flakes (server-side slow emit); give it room.
LISTING_READ_TIMEOUT = 300.0
# Observation payloads are small/medium and fast; keep the read timeout tight
# so a rare hung call can't eat the budget through retries.
OBS_READ_TIMEOUT = 90.0

SERIES_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("label", pa.string()),
    ("description", pa.string()),
    ("link", pa.string()),
])

VALUES_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("date", pa.string()),       # ISO "YYYY-MM-DD"; transform casts to date
    ("value", pa.float64()),
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
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=4, max=90),
    reraise=True,
)
def _get_json(url: str, params: dict | None = None, read_timeout: float = OBS_READ_TIMEOUT):
    # Docs advise polite throttling + exponential backoff; no hard rate limit.
    resp = get(url, params=params, timeout=(10.0, read_timeout))
    resp.raise_for_status()
    return resp.json()


def _san(sid: str) -> str:
    """Sanitize a series id into a filename-safe token."""
    return re.sub(r"[^a-z0-9]+", "-", sid.lower()).strip("-") or "x"


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _list_series() -> dict:
    """The full Valet series catalog as {series_id: {label, description, link}}."""
    payload = _get_json(f"{BASE}/lists/series/json", read_timeout=LISTING_READ_TIMEOUT)
    series = payload.get("series") or {}
    if not series:
        raise AssertionError("lists/series/json returned no series")
    return series


def _unpivot(payload: dict) -> list[dict]:
    """Wide date-joined rows -> long (series_id, date, value) rows.

    Each observation is {"d": date, "<series>": {"v": "<str>"}, ...}; a series
    absent on a date is simply omitted, and empty / non-numeric values are
    dropped (a missing observation is not a row)."""
    rows: list[dict] = []
    for obs in payload.get("observations", []):
        d = obs.get("d")
        if not d:
            continue
        for sid, cell in obs.items():
            if sid == "d":
                continue
            v = cell.get("v") if isinstance(cell, dict) else None
            if v is None or v == "":
                continue
            try:
                val = float(v)
            except (TypeError, ValueError):
                continue
            rows.append({"series_id": sid, "date": d, "value": val})
    return rows


def _fetch_obs(series_ids: list[str], start_date: str | None = None) -> list[dict]:
    """Fetch + un-pivot a chunk. A single discontinued/invalid id 404s the
    whole multi-series call (confirmed by probe), so on a 4xx for a batch we
    fall back to per-series and skip only the ids that 404."""
    names = ",".join(series_ids)
    params = {"start_date": start_date} if start_date else None
    try:
        return _unpivot(_get_json(f"{BASE}/observations/{names}/json", params=params))
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (400, 404) and len(series_ids) > 1:
            rows: list[dict] = []
            for sid in series_ids:
                try:
                    rows.extend(_unpivot(_get_json(
                        f"{BASE}/observations/{sid}/json", params=params)))
                except httpx.HTTPStatusError as e2:
                    if e2.response.status_code in (400, 404):
                        print(f"[values] skip {sid}: HTTP {e2.response.status_code}", flush=True)
                        continue
                    raise
            return rows
        raise


def fetch_series(entity_id: str) -> None:
    """Snapshot the full Valet series catalog into one parquet asset."""
    asset = f"bank-of-canada-{entity_id.lower().replace('_', '-')}"
    series = _list_series()
    rows = [
        {
            "series_id": sid,
            "label": meta.get("label"),
            "description": meta.get("description"),
            "link": meta.get("link"),
        }
        for sid, meta in series.items()
    ]
    table = pa.Table.from_pylist(rows, schema=SERIES_SCHEMA)
    save_raw_parquet(table, asset)
    print(f"[series] wrote {len(rows)} series to {asset}", flush=True)


def _backfill(state_key: str, remaining: list[str], cursor: str | None,
              date_watermark: str | None, deadline: float) -> None:
    """Walk the un-backfilled series in sorted order, fetching full history in
    REQUEST_BATCH chunks and flushing a parquet every FLUSH_EVERY_CHUNKS. The
    persisted cursor only advances on flush (raw written first), so a crash
    re-fetches at most one flush window — never silently skips a series."""
    buf: list[dict] = []
    range_start: str | None = None   # first id consumed in the current window
    consumed_last: str | None = None  # last id consumed (flushed or empty)
    chunks_since_flush = 0
    max_date = date_watermark
    flushed_files = 0
    n = len(remaining)

    def flush() -> None:
        nonlocal buf, range_start, consumed_last, chunks_since_flush, flushed_files, cursor
        if consumed_last is None:
            return
        if buf:
            key = f"{state_key}-{_san(range_start)}--{_san(consumed_last)}"
            save_raw_parquet(pa.Table.from_pylist(buf, schema=VALUES_SCHEMA), key)  # raw FIRST
            flushed_files += 1
        cursor = consumed_last  # advance resume point (over written-or-empty range)
        save_state(state_key, {                                                    # state AFTER raw
            "schema_version": STATE_VERSION,
            "cursor": cursor,
            "date_watermark": max_date,
            "last_run_stats": {
                "phase": "backfill", "cursor": cursor,
                "flushed_files": flushed_files, "rows_last_flush": len(buf),
            },
        })
        buf, range_start, consumed_last, chunks_since_flush = [], None, None, 0

    for i in range(0, n, REQUEST_BATCH):
        if time.monotonic() > deadline:
            print(f"[values] backfill budget reached at {i}/{n}, cursor={cursor}", flush=True)
            break
        chunk = remaining[i:i + REQUEST_BATCH]
        if range_start is None:
            range_start = chunk[0]
        rows = _fetch_obs(chunk)
        if rows:
            buf.extend(rows)
            cmax = max(r["date"] for r in rows)
            if max_date is None or cmax > max_date:
                max_date = cmax
        consumed_last = chunk[-1]
        chunks_since_flush += 1
        if chunks_since_flush >= FLUSH_EVERY_CHUNKS:
            flush()
        if (i // REQUEST_BATCH) % 25 == 0:
            print(f"[values] backfill chunk {i // REQUEST_BATCH} "
                  f"(series {i}/{n}), buffered={len(buf)}", flush=True)

    flush()  # remainder
    print(f"[values] backfill pass done: {flushed_files} files this run, cursor={cursor}", flush=True)


def _refresh(state_key: str, all_series: list[str], date_watermark: str | None,
             deadline: float) -> None:
    """Re-pull recent observations (>= watermark - overlap) for every series.
    Cheap (small per-series payloads); written as one or a few parquet parts."""
    base = date_watermark or (date.today() - timedelta(days=30)).isoformat()
    start = (date.fromisoformat(base) - timedelta(days=OVERLAP_DAYS)).isoformat()
    print(f"[values] refresh from start_date={start} across {len(all_series)} series", flush=True)

    buf: list[dict] = []
    max_date = date_watermark
    parts = 0
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    n = len(all_series)

    def flush() -> None:
        nonlocal buf, parts
        if not buf:
            return
        parts += 1
        save_raw_parquet(pa.Table.from_pylist(buf, schema=VALUES_SCHEMA),
                         f"{state_key}-r{run_ts}-{parts:03d}")
        buf = []

    for i in range(0, n, REQUEST_BATCH):
        if time.monotonic() > deadline:
            print(f"[values] refresh budget reached at {i}/{n}", flush=True)
            break
        rows = _fetch_obs(all_series[i:i + REQUEST_BATCH], start_date=start)
        if rows:
            buf.extend(rows)
            cmax = max(r["date"] for r in rows)
            if max_date is None or cmax > max_date:
                max_date = cmax
        if len(buf) >= FLUSH_EVERY_CHUNKS * 25_000:
            flush()
    flush()  # remainder; raw FIRST

    save_state(state_key, {                                       # state AFTER raw
        "schema_version": STATE_VERSION,
        "cursor": all_series[-1],   # stay in refresh; new tail series re-trigger backfill
        "date_watermark": max_date or base,
        "last_run_stats": {"phase": "refresh", "start_date": start, "files": parts},
    })
    print(f"[values] refresh done: {parts} files, watermark={max_date}", flush=True)


def fetch_values(entity_id: str) -> None:
    """Observations firehose. Backfill full history (cursor-paced) until the
    cursor reaches the end of the sorted catalog, then incremental refresh."""
    state_key = f"bank-of-canada-{entity_id.lower().replace('_', '-')}"
    state = load_state(state_key)
    if state and state.get("schema_version") != STATE_VERSION:
        print(f"[values] state schema_version {state.get('schema_version')} != "
              f"{STATE_VERSION}; resetting state", flush=True)
        state = {}

    cursor = state.get("cursor")              # last series_id fully backfilled (sorted)
    date_watermark = state.get("date_watermark")

    all_series = sorted(_list_series().keys())
    deadline = time.monotonic() + MAX_FETCH_SECONDS

    remaining = [s for s in all_series if cursor is None or s > cursor]
    if remaining:
        print(f"[values] backfill: {len(remaining)} of {len(all_series)} series remaining", flush=True)
        _backfill(state_key, remaining, cursor, date_watermark, deadline)
    else:
        _refresh(state_key, all_series, date_watermark, deadline)


DOWNLOAD_SPECS = [
    NodeSpec(id="bank-of-canada-series", fn=fetch_series, args=("series",), deps=(), kind="download"),
    NodeSpec(id="bank-of-canada-values", fn=fetch_values, args=("values",), deps=(), kind="download"),
]
