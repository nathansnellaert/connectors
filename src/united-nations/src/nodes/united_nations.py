"""United Nations — UNSD SDG Indicators Global Database (sdg_rest mechanism).

Two collect entities → two download specs:

  * ``series`` — the series catalog (one row per SDG time-series code).
    Single ``Series/List`` call, written as one NDJSON asset.

  * ``values`` — the long-format observations across every series, country and
    year (the SDG Global Database itself). This is a record-stream firehose:
    the API is row-bound (~0.009s/row; one 16.5k-row series takes ~2min) and
    the full corpus is millions of rows across 713 series, so a single fetch
    can NOT walk the whole thing in one refresh window. We batch per series
    code (``united-nations-values-<CODE>``), persist a completed-set watermark
    in state after every series, and cap each run with a soft time budget so a
    crash resumes instead of restarting. When a generation completes, the next
    invocation starts a fresh generation (re-pull) — the API exposes no
    incremental/``since`` filter, so revisions are only picked up by re-fetching.

No auth, no API key; the default subsets_utils User-Agent is accepted.
"""

import json
import time

import httpx
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
    save_raw_ndjson,
    load_state,
    save_state,
)

STATE_VERSION = 1

BASE = "https://unstats.un.org/SDGAPI/v1/sdg/"

# Row-bound API: larger pages don't improve throughput, they just raise
# per-request latency and memory. 5000 keeps each request well under the read
# timeout (~33s observed) while bounding in-flight memory per series.
PAGE_SIZE = 5000

# Runaway guard — a single series with >10M rows is impossible for SDG data;
# tripping this means the API or our pagination logic changed. Raise, never
# silently truncate.
MAX_PAGES = 2000

# Soft per-run budget for the values firehose. Hitting it returns cleanly with
# state advanced; the next refresh resumes from the completed-set watermark.
MAX_FETCH_SECONDS = 1500


# --------------------------------------------------------------------------- #
# HTTP with retry/backoff
# --------------------------------------------------------------------------- #

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
def _get_json(url: str, params: dict | None = None):
    resp = get(url, params=params, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp.json()


def _list_series() -> list[dict]:
    """The full Series/List catalog — one dict per series code."""
    data = _get_json(BASE + "Series/List")
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"Series/List returned unexpected payload: {type(data)}")
    return data


def _fetch_series_observations(code: str) -> list[dict]:
    """Walk every page of Series/Data for one series code."""
    rows: list[dict] = []
    page = 1
    while True:
        payload = _get_json(
            BASE + "Series/Data",
            params={"seriesCode": code, "pageSize": PAGE_SIZE, "pageNumber": page},
        )
        data = payload.get("data") or []
        rows.extend(data)
        total_pages = payload.get("totalPages") or 0
        if page >= total_pages or not data:
            break
        page += 1
        if page > MAX_PAGES:
            raise RuntimeError(
                f"series {code} exceeded MAX_PAGES={MAX_PAGES} "
                f"(totalPages={total_pages}) — pagination or source changed"
            )
    return rows


# --------------------------------------------------------------------------- #
# Flattening — drifty/nested records → stable scalar NDJSON
# --------------------------------------------------------------------------- #

def _first(v):
    """SDG taxonomy fields arrive as lists (a series can map to >1 goal);
    keep the primary element for the flat column."""
    if isinstance(v, list):
        return v[0] if v else None
    return v


def _flatten_series(s: dict) -> dict:
    return {
        "code": s.get("code"),
        "description": s.get("description"),
        "release": s.get("release"),
        "goal": ",".join(s.get("goal") or []),
        "target": ",".join(s.get("target") or []),
        "indicator": ",".join(s.get("indicator") or []),
        "uri": s.get("uri"),
    }


def _flatten_observation(r: dict, code: str) -> dict:
    attrs = r.get("attributes") or {}
    dims = r.get("dimensions") or {}
    return {
        "series_code": r.get("series") or code,
        "series_description": r.get("seriesDescription"),
        "goal": _first(r.get("goal")),
        "target": _first(r.get("target")),
        "indicator": _first(r.get("indicator")),
        "geo_area_code": r.get("geoAreaCode"),
        "geo_area_name": r.get("geoAreaName"),
        "time_period": r.get("timePeriodStart"),
        "value": r.get("value"),
        "value_type": r.get("valueType"),
        "upper_bound": r.get("upperBound"),
        "lower_bound": r.get("lowerBound"),
        "nature": attrs.get("Nature"),
        "units": attrs.get("Units"),
        "source": r.get("source"),
        # Disaggregation (Sex / Age / Reporting Type) varies per series — keep
        # as a stable JSON string so the NDJSON schema never drifts.
        "dimensions": json.dumps(dims, sort_keys=True) if dims else None,
    }


# --------------------------------------------------------------------------- #
# Fetch fns
# --------------------------------------------------------------------------- #

def fetch_series(node_id: str) -> None:
    """The series catalog — small, full re-pull every run."""
    asset = node_id  # "united-nations-series"
    rows = [_flatten_series(s) for s in _list_series()]
    save_raw_ndjson(rows, asset)
    print(f"[series] wrote {len(rows)} series to {asset}", flush=True)


def fetch_values(node_id: str) -> None:
    """The observation firehose — one NDJSON batch per series code, paced by a
    completed-set watermark and a soft per-run time budget."""
    state_key = node_id  # "united-nations-values"
    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        if state:
            print(f"[values] state schema_version mismatch; resetting", flush=True)
        state = {}

    completed = set(state.get("completed") or [])
    generation = state.get("generation", 0)

    codes = [s["code"] for s in _list_series() if s.get("code")]
    remaining = [c for c in codes if c not in completed]
    if not remaining:
        generation += 1
        completed = set()
        remaining = codes
        print(f"[values] prior generation complete; starting generation {generation}", flush=True)

    deadline = time.monotonic() + MAX_FETCH_SECONDS
    series_this_run = 0
    rows_this_run = 0

    for code in remaining:
        if time.monotonic() > deadline:
            print(
                f"[values] gen{generation} time budget hit after "
                f"{series_this_run} series this run ({len(completed)}/{len(codes)} total)",
                flush=True,
            )
            break

        try:
            observations = _fetch_series_observations(code)
        except httpx.HTTPStatusError as e:
            sc = e.response.status_code
            if sc != 429 and 400 <= sc < 500:
                # Permanent per-series failure: skip this series, keep going.
                print(f"[values] permanent {sc} for series {code}; skipping", flush=True)
                completed.add(code)
                save_state(state_key, {
                    "schema_version": STATE_VERSION,
                    "generation": generation,
                    "completed": sorted(completed),
                })
                continue
            raise

        # Write raw BEFORE advancing state, always.
        if observations:
            rows = [_flatten_observation(r, code) for r in observations]
            save_raw_ndjson(rows, f"united-nations-values-{code}")
            rows_this_run += len(rows)

        completed.add(code)
        series_this_run += 1
        save_state(state_key, {
            "schema_version": STATE_VERSION,
            "generation": generation,
            "completed": sorted(completed),
            "last_run_stats": {
                "series_this_run": series_this_run,
                "rows_this_run": rows_this_run,
            },
        })

        if series_this_run % 10 == 0:
            print(
                f"[values] gen{generation} {len(completed)}/{len(codes)} series done, "
                f"{rows_this_run} rows this run",
                flush=True,
            )


# --------------------------------------------------------------------------- #
# Specs
# --------------------------------------------------------------------------- #

DOWNLOAD_SPECS = [
    NodeSpec(id="united-nations-series", fn=fetch_series, kind="download"),
    NodeSpec(id="united-nations-values", fn=fetch_values, kind="download"),
]

TRANSFORM_SPECS = [
    SqlNodeSpec(
        id="united-nations-series-transform",
        deps=["united-nations-series"],
        sql='''
            SELECT
                code        AS series_code,
                description AS series_description,
                goal,
                target,
                indicator,
                release,
                uri
            FROM "united-nations-series"
            WHERE code IS NOT NULL
        ''',
    ),
    SqlNodeSpec(
        id="united-nations-values-transform",
        deps=["united-nations-values"],
        sql='''
            SELECT
                series_code,
                series_description,
                goal,
                target,
                indicator,
                CAST(geo_area_code AS VARCHAR) AS geo_area_code,
                geo_area_name,
                CAST(time_period AS INTEGER)   AS year,
                TRY_CAST(value AS DOUBLE)       AS value,
                value_type,
                TRY_CAST(upper_bound AS DOUBLE) AS upper_bound,
                TRY_CAST(lower_bound AS DOUBLE) AS lower_bound,
                nature,
                units,
                source,
                dimensions
            FROM "united-nations-values"
            WHERE value IS NOT NULL
              AND TRY_CAST(value AS DOUBLE) IS NOT NULL
              AND time_period IS NOT NULL
        ''',
    ),
]
