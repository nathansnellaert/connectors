"""USGS connector — two independent statistical surfaces under one source.

WATER  (9 entities): USGS Water Data OGC API Features
    https://api.waterdata.usgs.gov/ogcapi/v0/collections/<id>/items
    Each collection is a GeoJSON FeatureCollection paged via a cursor `next`
    link. The observation collections (continuous, daily, ...) are effectively
    unbounded and ever-growing, so each fetch is a *bounded* cursor crawl that
    resumes from a stored cursor and writes one NDJSON batch per page
    (firehose shape). Properties differ per collection and are full of nullable
    fields, so raw is NDJSON (drift-tolerant); the transform re-types per
    collection.

EARTHQUAKES (1 entity, `events`): USGS ComCat via the FDSN Event service
    https://earthquake.usgs.gov/fdsnws/event/1/query  (chosen mechanism)
    Full catalog is millions of events back to ~1900; a single query is capped
    at 20000 events. We window by time (30-day windows, offset-paged) and keep
    a two-pointer watermark: a forward pointer (refresh recent/new events on
    every run) and a backward pointer (progressively backfill history). CSV is
    the flattest format; we parse it to dicts and store NDJSON batches.

Freshness gating is the maintain step's job — these fns always fetch when run.
"""

import csv
import io
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs

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

# Bump when the shape of persisted state changes (cursor / watermark keys).
STATE_VERSION = 1

# ---------------------------------------------------------------------------
# Entity union (authoritative — copied from entity_union.json). `events` is the
# earthquake feed; the rest are water OGC-API collections.
# ---------------------------------------------------------------------------
EARTHQUAKE_ENTITIES = {"events"}
ENTITY_IDS = [
    "channel-measurements",
    "continuous",
    "daily",
    "events",
    "field-measurements",
    "latest-continuous",
    "latest-daily",
    "monitoring-locations",
    "peaks",
    "time-series-metadata",
]

# ---------------------------------------------------------------------------
# HTTP transport — retried, timed out, honest error classification.
# ---------------------------------------------------------------------------
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
def _http_get(url: str, params: dict) -> httpx.Response:
    resp = get(url, params=params, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp


# ===========================================================================
# WATER — cursor-paged OGC API Features crawl (firehose, NDJSON batches)
# ===========================================================================
WATER_BASE = "https://api.waterdata.usgs.gov/ogcapi/v0/collections"
WATER_PAGE_LIMIT = 5000        # features per page (API allows up to 20000)
WATER_MAX_PAGES_PER_RUN = 6    # soft per-run cap — deliberate pacing
WATER_MAX_RUN_SECONDS = 600    # soft wall-clock cap per run


def _extract_cursor(href: str) -> str | None:
    if not href:
        return None
    vals = parse_qs(urlparse(href).query).get("cursor")
    return vals[0] if vals else None


def _flatten_feature(feature: dict) -> dict:
    """One GeoJSON feature -> a flat dict: properties + geometry lon/lat."""
    props = dict(feature.get("properties") or {})
    geom = feature.get("geometry") or {}
    lon = lat = None
    if isinstance(geom, dict):
        coords = geom.get("coordinates")
        if isinstance(coords, list) and len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    props["_geometry_lon"] = lon
    props["_geometry_lat"] = lat
    if "id" not in props:
        # observation collections (continuous/daily) carry id at feature level
        props["_feature_id"] = feature.get("id")
    return props


def fetch_water(node_id: str) -> None:
    asset = node_id                          # the spec id IS the asset name
    entity = node_id[len("usgs-"):]
    items_url = f"{WATER_BASE}/{entity}/items"

    state = load_state(asset)
    if state.get("schema_version") != STATE_VERSION:
        state = {"schema_version": STATE_VERSION, "cursor": None, "batch_seq": 0}

    cursor = state.get("cursor")
    seq = state.get("batch_seq", 0)
    deadline = time.monotonic() + WATER_MAX_RUN_SECONDS
    pages = 0
    total = 0

    while pages < WATER_MAX_PAGES_PER_RUN and time.monotonic() < deadline:
        params = {"f": "json", "limit": WATER_PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor
        payload = _http_get(items_url, params).json()
        feats = payload.get("features") or []
        if not feats:
            # crawl exhausted — restart from the beginning on the next run so
            # the snapshot refreshes (batch_seq reset overwrites prior batches).
            state.update({"cursor": None, "batch_seq": 0})
            save_state(asset, state)
            break

        rows = [_flatten_feature(ft) for ft in feats]
        save_raw_ndjson(rows, f"{asset}-{seq:06d}")   # raw FIRST
        total += len(rows)
        seq += 1
        pages += 1

        nxt = next(
            (l.get("href") for l in payload.get("links", []) if l.get("rel") == "next"),
            None,
        )
        cursor = _extract_cursor(nxt)
        state.update({"cursor": cursor, "batch_seq": seq})
        save_state(asset, state)                       # then state

        if pages % 5 == 0:
            print(f"[{asset}] {pages} pages, {total} rows", flush=True)
        if cursor is None:
            # reached the last page exactly — restart next run.
            state.update({"cursor": None, "batch_seq": 0})
            save_state(asset, state)
            break

    state["last_run_stats"] = {"records": total, "pages": pages}
    save_state(asset, state)
    print(f"[{asset}] done: {pages} pages, {total} rows", flush=True)


# ===========================================================================
# EARTHQUAKES — time-windowed FDSN crawl (firehose, NDJSON batches)
# ===========================================================================
FDSN_QUERY = "https://earthquake.usgs.gov/fdsnws/event/1/query"
FDSN_PAGE = 20000                       # hard per-query cap of the service
FDSN_OFFSET_CAP = 2_000_000             # safety: a 30-day window exceeding this
EQ_WINDOW = timedelta(days=30)
EQ_OVERLAP = timedelta(days=2)          # re-fetch a little on forward refresh
EQ_SOURCE_MIN = datetime(1900, 1, 1, tzinfo=timezone.utc)
EQ_MAX_BACKFILL_WINDOWS = 4             # soft per-run backfill cap
EQ_MAX_RUN_SECONDS = 600


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _query_events(start: datetime, end: datetime) -> list[dict]:
    """All events in [start, end), offset-paged under the 20000 cap."""
    rows: list[dict] = []
    offset = 1
    while True:
        resp = _http_get(
            FDSN_QUERY,
            {
                "format": "csv",
                "starttime": _iso(start),
                "endtime": _iso(end),
                "orderby": "time",
                "limit": FDSN_PAGE,
                "offset": offset,
            },
        )
        text = resp.text
        if not text.strip():
            break                                  # 204 / empty window
        page = [dict(r) for r in csv.DictReader(io.StringIO(text))]
        if not page:
            break
        rows.extend(page)
        if len(page) < FDSN_PAGE:
            break
        offset += FDSN_PAGE
        if offset > FDSN_OFFSET_CAP:
            raise RuntimeError(
                f"FDSN window {_iso(start)}..{_iso(end)} exceeds offset cap "
                f"{FDSN_OFFSET_CAP}; window must be narrowed."
            )
    return rows


def _save_event_batch(node_id: str, start: datetime, end: datetime, rows: list[dict]) -> None:
    batch_key = f"{start.date().isoformat()}-{end.date().isoformat()}"
    save_raw_ndjson(rows, f"{node_id}-{batch_key}")


def fetch_earthquakes(node_id: str) -> None:
    asset = node_id
    state = load_state(asset)
    if state.get("schema_version") != STATE_VERSION:
        state = {"schema_version": STATE_VERSION}

    now = datetime.now(timezone.utc).replace(microsecond=0)
    deadline = time.monotonic() + EQ_MAX_RUN_SECONDS
    total = 0

    # --- forward: refresh recent / newly-arrived events every run ---
    newest = state.get("newest_seen")
    fwd_start = (
        datetime.fromisoformat(newest) - EQ_OVERLAP if newest else now - EQ_WINDOW
    )
    rows = _query_events(fwd_start, now)
    if rows:
        _save_event_batch(node_id, fwd_start, now, rows)   # raw FIRST
        total += len(rows)
    state["newest_seen"] = now.isoformat()
    save_state(asset, state)

    # --- backfill: march the history pointer backward toward SOURCE_MIN ---
    frontier = (
        datetime.fromisoformat(state["oldest_start"])
        if state.get("oldest_start")
        else fwd_start
    )
    windows = 0
    while (
        frontier > EQ_SOURCE_MIN
        and windows < EQ_MAX_BACKFILL_WINDOWS
        and time.monotonic() < deadline
    ):
        w_start = max(frontier - EQ_WINDOW, EQ_SOURCE_MIN)
        rows = _query_events(w_start, frontier)
        if rows:
            _save_event_batch(node_id, w_start, frontier, rows)  # raw FIRST
            total += len(rows)
        frontier = w_start
        state["oldest_start"] = frontier.isoformat()
        save_state(asset, state)                                  # then state
        windows += 1
        print(f"[{asset}] backfilled to {frontier.date()}, {total} rows", flush=True)

    state["last_run_stats"] = {"records": total}
    save_state(asset, state)
    print(f"[{asset}] done: {total} rows", flush=True)


# ---------------------------------------------------------------------------
# DOWNLOAD_SPECS — one per entity-union entry.
# ---------------------------------------------------------------------------
def _fetch_for(entity_id: str):
    return fetch_earthquakes if entity_id in EARTHQUAKE_ENTITIES else fetch_water


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"usgs-{eid.lower().replace('_', '-')}",
        fn=_fetch_for(eid),
        kind="download",
    )
    for eid in ENTITY_IDS
]


# ===========================================================================
# TRANSFORM_SPECS — one published Delta table per subset. Thin parse/type +
# dedup of overlap duplicates (QUALIFY row_number by natural key, newest wins).
# ===========================================================================
_TRANSFORM_SQL = {
    "channel-measurements": '''
        SELECT
            CAST(id AS VARCHAR)                       AS id,
            CAST(monitoring_location_id AS VARCHAR)   AS monitoring_location_id,
            CAST(field_visit_id AS VARCHAR)           AS field_visit_id,
            TRY_CAST(measurement_number AS BIGINT)    AS measurement_number,
            TRY_CAST("time" AS TIMESTAMP)             AS time,
            channel_name,
            TRY_CAST(channel_flow AS DOUBLE)          AS channel_flow,
            channel_flow_unit,
            TRY_CAST(channel_width AS DOUBLE)         AS channel_width,
            TRY_CAST(channel_area AS DOUBLE)          AS channel_area,
            TRY_CAST(channel_velocity AS DOUBLE)      AS channel_velocity,
            measurement_type,
            TRY_CAST(_geometry_lon AS DOUBLE)         AS longitude,
            TRY_CAST(_geometry_lat AS DOUBLE)         AS latitude,
            TRY_CAST(last_modified AS TIMESTAMP)      AS last_modified
        FROM "{dep}"
        WHERE id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY id
            ORDER BY TRY_CAST(last_modified AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
    "continuous": '''
        SELECT
            CAST(time_series_id AS VARCHAR)           AS time_series_id,
            CAST(monitoring_location_id AS VARCHAR)   AS monitoring_location_id,
            CAST(parameter_code AS VARCHAR)           AS parameter_code,
            CAST(statistic_id AS VARCHAR)             AS statistic_id,
            TRY_CAST("time" AS TIMESTAMP)             AS time,
            TRY_CAST("value" AS DOUBLE)               AS value,
            unit_of_measure,
            approval_status,
            qualifier,
            TRY_CAST(_geometry_lon AS DOUBLE)         AS longitude,
            TRY_CAST(_geometry_lat AS DOUBLE)         AS latitude,
            TRY_CAST(last_modified AS TIMESTAMP)      AS last_modified
        FROM "{dep}"
        WHERE time_series_id IS NOT NULL AND "time" IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY time_series_id, "time"
            ORDER BY TRY_CAST(last_modified AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
    "daily": '''
        SELECT
            CAST(time_series_id AS VARCHAR)           AS time_series_id,
            CAST(monitoring_location_id AS VARCHAR)   AS monitoring_location_id,
            CAST(parameter_code AS VARCHAR)           AS parameter_code,
            CAST(statistic_id AS VARCHAR)             AS statistic_id,
            TRY_CAST("time" AS DATE)                  AS date,
            TRY_CAST("value" AS DOUBLE)               AS value,
            unit_of_measure,
            approval_status,
            qualifier,
            TRY_CAST(_geometry_lon AS DOUBLE)         AS longitude,
            TRY_CAST(_geometry_lat AS DOUBLE)         AS latitude,
            TRY_CAST(last_modified AS TIMESTAMP)      AS last_modified
        FROM "{dep}"
        WHERE time_series_id IS NOT NULL AND "time" IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY time_series_id, "time"
            ORDER BY TRY_CAST(last_modified AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
    "field-measurements": '''
        SELECT
            CAST(field_measurements_series_id AS VARCHAR) AS field_measurements_series_id,
            CAST(monitoring_location_id AS VARCHAR)       AS monitoring_location_id,
            CAST(parameter_code AS VARCHAR)               AS parameter_code,
            reading_type,
            CAST(field_visit_id AS VARCHAR)               AS field_visit_id,
            TRY_CAST("value" AS DOUBLE)                   AS value,
            unit_of_measure,
            TRY_CAST("time" AS TIMESTAMP)                 AS time,
            approval_status,
            qualifier,
            TRY_CAST(_geometry_lon AS DOUBLE)             AS longitude,
            TRY_CAST(_geometry_lat AS DOUBLE)             AS latitude,
            TRY_CAST(last_modified AS TIMESTAMP)          AS last_modified
        FROM "{dep}"
        WHERE field_measurements_series_id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY field_measurements_series_id, parameter_code
            ORDER BY TRY_CAST(last_modified AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
    "latest-continuous": '''
        SELECT
            CAST(id AS VARCHAR)                       AS id,
            CAST(time_series_id AS VARCHAR)           AS time_series_id,
            CAST(monitoring_location_id AS VARCHAR)   AS monitoring_location_id,
            CAST(parameter_code AS VARCHAR)           AS parameter_code,
            CAST(statistic_id AS VARCHAR)             AS statistic_id,
            TRY_CAST("time" AS TIMESTAMP)             AS time,
            TRY_CAST("value" AS DOUBLE)               AS value,
            unit_of_measure,
            approval_status,
            qualifier,
            TRY_CAST(_geometry_lon AS DOUBLE)         AS longitude,
            TRY_CAST(_geometry_lat AS DOUBLE)         AS latitude,
            TRY_CAST(last_modified AS TIMESTAMP)      AS last_modified
        FROM "{dep}"
        WHERE id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY id
            ORDER BY TRY_CAST(last_modified AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
    "latest-daily": '''
        SELECT
            CAST(id AS VARCHAR)                       AS id,
            CAST(time_series_id AS VARCHAR)           AS time_series_id,
            CAST(monitoring_location_id AS VARCHAR)   AS monitoring_location_id,
            CAST(parameter_code AS VARCHAR)           AS parameter_code,
            CAST(statistic_id AS VARCHAR)             AS statistic_id,
            TRY_CAST("time" AS DATE)                  AS date,
            TRY_CAST("value" AS DOUBLE)               AS value,
            unit_of_measure,
            approval_status,
            qualifier,
            TRY_CAST(_geometry_lon AS DOUBLE)         AS longitude,
            TRY_CAST(_geometry_lat AS DOUBLE)         AS latitude,
            TRY_CAST(last_modified AS TIMESTAMP)      AS last_modified
        FROM "{dep}"
        WHERE id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY id
            ORDER BY TRY_CAST(last_modified AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
    "monitoring-locations": '''
        SELECT
            CAST(id AS VARCHAR)                       AS id,
            agency_code,
            agency_name,
            monitoring_location_number,
            monitoring_location_name,
            country_code,
            country_name,
            state_code,
            state_name,
            county_code,
            county_name,
            site_type_code,
            site_type,
            hydrologic_unit_code,
            TRY_CAST(altitude AS DOUBLE)              AS altitude,
            TRY_CAST(drainage_area AS DOUBLE)         AS drainage_area,
            TRY_CAST(_geometry_lon AS DOUBLE)         AS longitude,
            TRY_CAST(_geometry_lat AS DOUBLE)         AS latitude,
            TRY_CAST(revision_modified AS TIMESTAMP)  AS revision_modified
        FROM "{dep}"
        WHERE id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY id
            ORDER BY TRY_CAST(revision_modified AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
    "peaks": '''
        SELECT
            CAST(id AS VARCHAR)                       AS id,
            CAST(time_series_id AS VARCHAR)           AS time_series_id,
            CAST(monitoring_location_id AS VARCHAR)   AS monitoring_location_id,
            CAST(parameter_code AS VARCHAR)           AS parameter_code,
            TRY_CAST("value" AS DOUBLE)               AS value,
            unit_of_measure,
            TRY_CAST("time" AS TIMESTAMP)             AS time,
            TRY_CAST(water_year AS BIGINT)            AS water_year,
            peak_since,
            qualifier,
            TRY_CAST(_geometry_lon AS DOUBLE)         AS longitude,
            TRY_CAST(_geometry_lat AS DOUBLE)         AS latitude,
            TRY_CAST(last_modified AS TIMESTAMP)      AS last_modified
        FROM "{dep}"
        WHERE id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY id
            ORDER BY TRY_CAST(last_modified AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
    "time-series-metadata": '''
        SELECT
            CAST(id AS VARCHAR)                       AS id,
            CAST(monitoring_location_id AS VARCHAR)   AS monitoring_location_id,
            CAST(parameter_code AS VARCHAR)           AS parameter_code,
            parameter_name,
            parameter_description,
            CAST(statistic_id AS VARCHAR)             AS statistic_id,
            unit_of_measure,
            hydrologic_unit_code,
            state_name,
            TRY_CAST("begin" AS TIMESTAMP)            AS begin_time,
            TRY_CAST("end" AS TIMESTAMP)              AS end_time,
            computation_identifier,
            computation_period_identifier,
            web_description,
            CAST(parent_time_series_id AS VARCHAR)    AS parent_time_series_id,
            TRY_CAST(last_modified AS TIMESTAMP)      AS last_modified
        FROM "{dep}"
        WHERE id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY id
            ORDER BY TRY_CAST(last_modified AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
    "events": '''
        SELECT
            CAST(id AS VARCHAR)                       AS id,
            TRY_CAST("time" AS TIMESTAMP)             AS time,
            TRY_CAST(latitude AS DOUBLE)              AS latitude,
            TRY_CAST(longitude AS DOUBLE)             AS longitude,
            TRY_CAST(depth AS DOUBLE)                 AS depth,
            TRY_CAST(mag AS DOUBLE)                   AS magnitude,
            "magType"                                 AS magnitude_type,
            TRY_CAST(nst AS BIGINT)                   AS nst,
            TRY_CAST(gap AS DOUBLE)                   AS gap,
            TRY_CAST(dmin AS DOUBLE)                  AS dmin,
            TRY_CAST(rms AS DOUBLE)                   AS rms,
            net,
            place,
            "type"                                    AS event_type,
            status,
            "locationSource"                          AS location_source,
            "magSource"                               AS mag_source,
            TRY_CAST(updated AS TIMESTAMP)            AS updated
        FROM "{dep}"
        WHERE id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY id
            ORDER BY TRY_CAST(updated AS TIMESTAMP) DESC NULLS LAST
        ) = 1
    ''',
}


def _transform_for(spec: NodeSpec) -> SqlNodeSpec:
    entity = spec.id[len("usgs-"):]
    return SqlNodeSpec(
        id=f"{spec.id}-transform",
        deps=[spec.id],
        sql=_TRANSFORM_SQL[entity].format(dep=spec.id),
    )


TRANSFORM_SPECS = [_transform_for(s) for s in DOWNLOAD_SPECS]
