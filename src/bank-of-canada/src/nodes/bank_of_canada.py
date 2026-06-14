"""Bank of Canada Valet API connector.

Mechanism: valet_rest (https://www.bankofcanada.ca/valet/) — no auth, no key.

Two collect entities are published:
  * series  — the catalog of every time series (id, label, description, link),
              fetched in one shot from /lists/series/json.
  * values  — the long-format observations across ALL ~15.6k series.

Shape decision (values): the corpus is large (~15.6k series, full history each),
so a single in-memory table is not viable. We fetch observations in stable
CRC32-hash buckets of series ids via the multi-series endpoint
(/observations/{a,b,c}/json, which merges members by date in one response) and
write one parquet batch per bucket (asset id `bank-of-canada-values-bucket-NNN`).
The SQL transform's dep view globs `bank-of-canada-values-*` and unions them.

This is a STATELESS FULL RE-PULL: every run rewrites every bucket file (fixed
bucket count, all overwritten), so revisions/late corrections are picked up for
free and there are no orphaned batch files. Incremental query IS supported by
the API (start_date/recent), but per-bucket watermarks over a shifting series
list buy little here and would forfeit revision capture — re-pull is simpler and
correct. The maintain step (authored later) gates how often this runs.
"""

import zlib

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
    SqlNodeSpec,
    get,
    save_raw_parquet,
    save_state,
)

BASE = "https://www.bankofcanada.ca/valet"

# Number of fixed hash buckets the ~15.6k series are split into for fetching the
# `values` entity. ~49 series/bucket today; grows slowly with the corpus. Fixed
# count => fixed set of batch file names, all overwritten each run (no orphans).
N_BUCKETS = 320

# Raw is stored long-format with values as strings (the API returns numeric
# strings); the transform casts. date kept as 'YYYY-MM-DD' string, cast in SQL.
VALUES_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("date", pa.string()),
    ("value", pa.string()),
])

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


# No published hard limit; docs advise a gradual request rate + backoff. Keep a
# conservative polite ceiling (~a few rps) for the multi-hundred-call crawl.
@sleep_and_retry
@limits(calls=5, period=1)
def _throttle() -> None:
    return None


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=2, max=60),
    reraise=True,
)
def _fetch_json(url: str, params: dict | None = None) -> dict:
    _throttle()
    resp = get(url, params=params, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp.json()


def _parse_observations(payload: dict) -> list[dict]:
    """Flatten Valet observation rows {d, <series>: {v}} to long-format dicts."""
    rows: list[dict] = []
    for obs in payload.get("observations", []):
        d = obs.get("d")
        if not d:
            continue
        for key, cell in obs.items():
            if key == "d":
                continue
            if isinstance(cell, dict):
                val = cell.get("v")
                if val is not None and val != "":
                    rows.append({"series_id": key, "date": d, "value": str(val)})
    return rows


def _fetch_bucket_rows(series_ids: list[str]) -> list[dict]:
    """Fetch observations for a bucket of series.

    Tries the multi-series endpoint first. If the whole call 404s (a member was
    delisted between listing and fetch), fall back to per-series fetches so one
    bad id doesn't drop the whole bucket.
    """
    url = f"{BASE}/observations/{','.join(series_ids)}/json"
    try:
        return _parse_observations(_fetch_json(url))
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            raise
        print(
            f"[bank-of-canada] bucket multi-fetch 404 ({len(series_ids)} series) "
            f"-> per-series fallback: {url}",
            flush=True,
        )

    rows: list[dict] = []
    for sid in series_ids:
        one_url = f"{BASE}/observations/{sid}/json"
        try:
            rows.extend(_parse_observations(_fetch_json(one_url)))
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                print(
                    f"[bank-of-canada] skipping delisted series {sid} (404)",
                    flush=True,
                )
                continue
            raise
    return rows


def fetch_series(node_id: str) -> None:
    """Download the full series catalog from /lists/series/json (one asset)."""
    asset = node_id
    payload = _fetch_json(f"{BASE}/lists/series/json")
    series = payload.get("series", {})
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
    save_state(node_id, {"schema_version": 1, "last_run_stats": {"records": len(rows)}})


def fetch_values(node_id: str) -> None:
    """Download all observations, bucketed by CRC32(series_id) % N_BUCKETS.

    Writes one parquet batch per bucket: `bank-of-canada-values-bucket-NNN`.
    """
    payload = _fetch_json(f"{BASE}/lists/series/json")
    all_ids = sorted(payload.get("series", {}).keys())
    if not all_ids:
        raise AssertionError("series list empty — Valet /lists/series returned no series")

    buckets: dict[int, list[str]] = {}
    for sid in all_ids:
        b = zlib.crc32(sid.encode("utf-8")) % N_BUCKETS
        buckets.setdefault(b, []).append(sid)

    total_rows = 0
    for i in range(N_BUCKETS):
        ids = buckets.get(i)
        if not ids:
            continue
        rows = _fetch_bucket_rows(ids)
        table = pa.Table.from_pylist(rows, schema=VALUES_SCHEMA)
        save_raw_parquet(table, f"{node_id}-bucket-{i:03d}")
        total_rows += len(rows)
        if (i + 1) % 20 == 0:
            print(
                f"[bank-of-canada] values bucket {i + 1}/{N_BUCKETS} done, "
                f"{total_rows} rows so far",
                flush=True,
            )

    save_state(
        node_id,
        {
            "schema_version": 1,
            "last_run_stats": {"series": len(all_ids), "records": total_rows},
        },
    )


DOWNLOAD_SPECS = [
    NodeSpec(id="bank-of-canada-series", fn=fetch_series, kind="download"),
    NodeSpec(id="bank-of-canada-values", fn=fetch_values, kind="download"),
]

TRANSFORM_SPECS = [
    SqlNodeSpec(
        id="bank-of-canada-series-transform",
        deps=["bank-of-canada-series"],
        sql='''
            SELECT
                series_id,
                label,
                description,
                link
            FROM "bank-of-canada-series"
            WHERE series_id IS NOT NULL
        ''',
    ),
    SqlNodeSpec(
        id="bank-of-canada-values-transform",
        deps=["bank-of-canada-values"],
        sql='''
            SELECT
                series_id,
                CAST(date AS DATE)          AS date,
                TRY_CAST(value AS DOUBLE)   AS value
            FROM "bank-of-canada-values"
            WHERE series_id IS NOT NULL
              AND date IS NOT NULL
              AND TRY_CAST(value AS DOUBLE) IS NOT NULL
        ''',
    ),
]
