"""Bank of Canada — Valet REST download nodes.

Two entities (the rank-approved union):

  * ``series`` — the full Valet series catalog (one row per series: id, label,
    description, link). A snapshot; refetched + overwritten each run.
  * ``values`` — long-format observations across every series
    (one row per (series_id, date, value)). A bounded firehose: ~15.5k series,
    each call returning that series' full history. Backfilled across runs via a
    series-id cursor, then refreshed incrementally with ``start_date``.

Mechanism: ``valet_rest`` — no auth, no key. Observations for many series are
fetched in one call via the comma-separated ``/observations/{names}/json``
endpoint; the wide, date-joined response is un-pivoted back to long rows.
"""

import time
from datetime import datetime, timedelta, timezone

import httpx
import pyarrow as pa
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from subsets_utils import NodeSpec, get, save_raw_parquet, load_state, save_state

BASE = "https://www.bankofcanada.ca/valet"

# Watermark/cursor contract version. Bump when the `values` state shape changes
# so stale state from an older shape is discarded rather than misread.
STATE_VERSION = 1

# Series per multi-series observation request. URL stays well under server
# limits (~750 chars for 50 ids vs the ~8k typical cap), and one chunk's
# full-history un-pivot stays comfortably within subprocess RAM.
BATCH_SIZE = 50

# Soft per-run budget for the `values` firehose. Backfill resumes from its
# series-id cursor on the next refresh, so hitting this just paces the crawl.
MAX_FETCH_SECONDS = 1500

# Refresh re-fetch window: re-pull recent observations to absorb late
# revisions. Duplicates are dedup'd downstream by the (series_id, date) merge.
OVERLAP_DAYS = 14

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
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _get_json(url: str, params: dict | None = None):
    # Docs advise polite throttling + exponential backoff; no hard limit.
    resp = get(url, params=params, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _fetch_chunk_payload(series_ids: list[str], start_date: str | None = None):
    names = ",".join(series_ids)
    params = {"start_date": start_date} if start_date else None
    return _get_json(f"{BASE}/observations/{names}/json", params=params)


def _unpivot(payload: dict) -> list[dict]:
    """Wide date-joined rows -> long (series_id, date, value) rows.

    Each observation is {"d": date, "<series>": {"v": "<str>"}, ...}; a series
    absent on a date is simply omitted. Empty / non-numeric values are dropped
    (a missing observation is not a row)."""
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
            except (ValueError, TypeError):
                continue
            rows.append({"series_id": sid, "date": d, "value": val})
    return rows


def _fetch_obs_rows(series_ids: list[str], start_date: str | None = None) -> list[dict]:
    """Fetch + un-pivot a chunk. On a 4xx for the whole batch, fall back to
    per-series so one discontinued id doesn't sink the batch."""
    try:
        return _unpivot(_fetch_chunk_payload(series_ids, start_date))
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (400, 404) and len(series_ids) > 1:
            rows: list[dict] = []
            for sid in series_ids:
                try:
                    rows.extend(_unpivot(_fetch_chunk_payload([sid], start_date)))
                except httpx.HTTPStatusError as e2:
                    if e2.response.status_code in (400, 404):
                        print(f"[values] skipping {sid}: HTTP {e2.response.status_code}", flush=True)
                        continue
                    raise
            return rows
        raise


def fetch_series(entity_id: str) -> None:
    """Snapshot of the full Valet series catalog -> one parquet asset."""
    asset = f"bank-of-canada-{entity_id.lower().replace('_', '-')}"
    payload = _get_json(f"{BASE}/lists/series/json")
    series = payload.get("series", {})
    if not series:
        raise AssertionError("lists/series returned no series")
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


def fetch_values(entity_id: str) -> None:
    """Observations firehose. Backfill (full history, cursor-paced) until every
    series is covered, then incremental refresh via ``start_date``."""
    state_key = f"bank-of-canada-{entity_id.lower().replace('_', '-')}"
    state = load_state(state_key)
    if state and state.get("schema_version") != STATE_VERSION:
        print(f"[values] state schema_version {state.get('schema_version')} != "
              f"{STATE_VERSION}; resetting state", flush=True)
        state = {}

    backfill_done = state.get("backfill_done", False)
    backfill_cursor = state.get("backfill_cursor")   # last series_id fully backfilled
    date_watermark = state.get("date_watermark")     # max observation date seen
    batch_seq = state.get("batch_seq", 0)

    # Discover the full series universe. Sorted so the cursor has stable,
    # resumable semantics across runs even as the catalog grows.
    listing = _get_json(f"{BASE}/lists/series/json")
    all_series = sorted(listing.get("series", {}).keys())
    if not all_series:
        raise AssertionError("lists/series returned no series")

    deadline = time.monotonic() + MAX_FETCH_SECONDS
    max_date = date_watermark

    if not backfill_done:
        remaining = [s for s in all_series if backfill_cursor is None or s > backfill_cursor]
        print(f"[values] backfill: {len(remaining)} of {len(all_series)} series remaining", flush=True)
        if not remaining:
            state = {
                "schema_version": STATE_VERSION, "backfill_done": True,
                "backfill_cursor": backfill_cursor, "date_watermark": date_watermark or _today(),
                "batch_seq": batch_seq,
            }
            save_state(state_key, state)
            return

        total_rows = 0
        completed = True
        for i in range(0, len(remaining), BATCH_SIZE):
            if time.monotonic() > deadline:
                print(f"[values] backfill budget reached; pausing at cursor={backfill_cursor}", flush=True)
                completed = False
                break
            chunk = remaining[i:i + BATCH_SIZE]
            rows = _fetch_obs_rows(chunk)            # full history
            if rows:
                batch_seq += 1
                table = pa.Table.from_pylist(rows, schema=VALUES_SCHEMA)
                save_raw_parquet(table, f"{state_key}-b{batch_seq:08d}")   # raw FIRST
                total_rows += len(rows)
                cmax = max(r["date"] for r in rows)
                if max_date is None or cmax > max_date:
                    max_date = cmax
            backfill_cursor = chunk[-1]
            state = {
                "schema_version": STATE_VERSION, "backfill_done": False,
                "backfill_cursor": backfill_cursor, "date_watermark": max_date,
                "batch_seq": batch_seq,
                "last_run_stats": {"phase": "backfill", "rows": total_rows, "cursor": backfill_cursor},
            }
            save_state(state_key, state)             # state AFTER raw
            if (i // BATCH_SIZE) % 20 == 0:
                print(f"[values] backfill chunk {i // BATCH_SIZE}, cursor={backfill_cursor}, rows so far={total_rows}", flush=True)

        if completed:
            print(f"[values] backfill complete ({len(all_series)} series, watermark={max_date})", flush=True)
            state = {
                "schema_version": STATE_VERSION, "backfill_done": True,
                "backfill_cursor": backfill_cursor, "date_watermark": max_date or _today(),
                "batch_seq": batch_seq,
                "last_run_stats": {"phase": "backfill", "rows": total_rows, "cursor": backfill_cursor},
            }
            save_state(state_key, state)
        return

    # --- refresh phase: re-pull recent observations across all series ---
    base_date = date_watermark or "1900-01-01"
    start_date = (datetime.strptime(base_date, "%Y-%m-%d").date() - timedelta(days=OVERLAP_DAYS)).isoformat()
    print(f"[values] refresh from start_date={start_date} across {len(all_series)} series", flush=True)

    all_rows: list[dict] = []
    n_chunks = (len(all_series) + BATCH_SIZE - 1) // BATCH_SIZE
    for idx, i in enumerate(range(0, len(all_series), BATCH_SIZE)):
        if time.monotonic() > deadline:
            print(f"[values] refresh budget reached at chunk {idx}/{n_chunks}", flush=True)
            break
        all_rows.extend(_fetch_obs_rows(all_series[i:i + BATCH_SIZE], start_date=start_date))
        if idx % 20 == 0:
            print(f"[values] refresh chunk {idx}/{n_chunks}, rows so far={len(all_rows)}", flush=True)

    if all_rows:
        run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        table = pa.Table.from_pylist(all_rows, schema=VALUES_SCHEMA)
        save_raw_parquet(table, f"{state_key}-r{run_ts}")            # raw FIRST
        cmax = max(r["date"] for r in all_rows)
        if max_date is None or cmax > max_date:
            max_date = cmax

    state = {
        "schema_version": STATE_VERSION, "backfill_done": True,
        "backfill_cursor": backfill_cursor, "date_watermark": max_date or base_date,
        "batch_seq": batch_seq,
        "last_run_stats": {"phase": "refresh", "rows": len(all_rows), "start_date": start_date},
    }
    save_state(state_key, state)                                     # state AFTER raw


DOWNLOAD_SPECS = [
    NodeSpec(id="bank-of-canada-series", fn=fetch_series, args=("series",), deps=(), kind="download"),
    NodeSpec(id="bank-of-canada-values", fn=fetch_values, args=("values",), deps=(), kind="download"),
]
