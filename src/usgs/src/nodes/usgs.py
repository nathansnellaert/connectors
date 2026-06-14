"""USGS connector — two independent statistical surfaces under one source.

WATER  (9 entities): USGS Water Data OGC API Features
    https://api.waterdata.usgs.gov/ogcapi/v0/collections/<id>/items
    Each collection is a GeoJSON FeatureCollection paged via a cursor `next`
    link. The observation collections (continuous, daily, ...) are effectively
    unbounded (billions of observations) and there is no whole-corpus bulk dump
    nor a stable monotonic key we can resume against (the cursor is opaque), so
    a full re-pull is infeasible and a resumable watermark is not well-defined.
    We therefore publish a *bounded rolling snapshot*: a deterministic crawl of
    the first WATER_MAX_PAGES pages from the start of each collection, one
    NDJSON batch per page. Re-running overwrites the same batch files
    (idempotent). Properties differ per collection and are full of nullable /
    occasionally-nested fields, so raw is NDJSON (drift-tolerant) with non-scalar
    property values JSON-encoded to keep every column SQL-readable; the transform
    re-types per collection.

EARTHQUAKES (1 entity, `events`): USGS ComCat via the FDSN Event service
    https://earthquake.usgs.gov/fdsnws/event/1/query  (chosen mechanism)
    Full catalog is millions of events back to ~1900; a single query is capped
    at 20000 events. There is no bulk dump, so we publish a bounded rolling
    snapshot of the last EQ_MONTHS_BACK calendar months (all magnitudes), one
    NDJSON batch per month, each window offset-paged under the 20000 cap. CSV is
    the flattest format; we parse it to dicts. Re-running overwrites the same
    month batches (idempotent); the current partial month refreshes each run.

Freshness gating is the maintain step's job — these fns always fetch when run.
Both surfaces are stateless full-snapshot crawls: no watermark, no cursor
persistence, no terminal flags. Robust under the harness's parallel spawn
execution and trivially idempotent.
"""

import csv
import io
from datetime import datetime, timezone
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
)

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
# HTTP transport — retried, timed out, honest error classification. Research
# notes the water API answers excessive use with 403/429, so 403 is treated as
# a transient rate-limit signal (back off and retry); other 4xx stay permanent.
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
        return code in (403, 429) or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(7),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _http_get(url: str, params: dict) -> httpx.Response:
    resp = get(url, params=params, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp


# ===========================================================================
# WATER — cursor-paged OGC API Features crawl (bounded snapshot, NDJSON batches)
# ===========================================================================
WATER_BASE = "https://api.waterdata.usgs.gov/ogcapi/v0/collections"
WATER_PAGE_LIMIT = 5000        # features per page (API allows up to 20000)
WATER_MAX_PAGES = 6            # bounded snapshot depth — see module docstring


def _extract_cursor(href: str) -> str | None:
    if not href:
        return None
    vals = parse_qs(urlparse(href).query).get("cursor")
    return vals[0] if vals else None


def _scalarize(value):
    """Keep scalars; JSON-encode dict/list values so every NDJSON column stays a
    flat, uniformly-typed scalar that DuckDB's read_json can union cleanly."""
    if isinstance(value, (dict, list)):
        import json
        return json.dumps(value, separators=(",", ":"))
    return value


def _flatten_feature(feature: dict) -> dict:
    """One GeoJSON feature -> a flat dict: properties + geometry lon/lat."""
    props = {k: _scalarize(v) for k, v in (feature.get("properties") or {}).items()}
    geom = feature.get("geometry") or {}
    lon = lat = None
    if isinstance(geom, dict):
        coords = geom.get("coordinates")
        if isinstance(coords, list) and len(coords) >= 2:
            lon, lat = coords[0], coords[1]
    props["_geometry_lon"] = lon
    props["_geometry_lat"] = lat
    if "id" not in props:
        # observation collections carry their id at the feature level
        props["_feature_id"] = feature.get("id")
    return props


def fetch_water(node_id: str) -> None:
    asset = node_id                          # the spec id IS the asset name
    entity = node_id[len("usgs-"):]
    items_url = f"{WATER_BASE}/{entity}/items"

    cursor = None
    total = 0
    for seq in range(WATER_MAX_PAGES):
        params = {"f": "json", "limit": WATER_PAGE_LIMIT}
        if cursor:
            params["cursor"] = cursor
        payload = _http_get(items_url, params).json()
        feats = payload.get("features") or []
        if not feats:
            break                            # collection exhausted before the cap

        rows = [_flatten_feature(ft) for ft in feats]
        save_raw_ndjson(rows, f"{asset}-{seq:06d}")   # raw write per page
        total += len(rows)
        print(f"[{asset}] page {seq + 1}/{WATER_MAX_PAGES}, {total} rows", flush=True)

        nxt = next(
            (l.get("href") for l in payload.get("links", []) if l.get("rel") == "next"),
            None,
        )
        cursor = _extract_cursor(nxt)
        if cursor is None:
            break                            # reached the last page exactly

    if total == 0:
        # A collection that returns zero features on its very first page is a
        # real upstream change, not normal — surface it loudly.
        raise RuntimeError(f"{asset}: water collection returned no features")
    print(f"[{asset}] done: {total} rows", flush=True)


# ===========================================================================
# EARTHQUAKES — month-windowed FDSN crawl (bounded snapshot, NDJSON batches)
# ===========================================================================
FDSN_QUERY = "https://earthquake.usgs.gov/fdsnws/event/1/query"
FDSN_PAGE = 20000                       # hard per-query cap of the service
FDSN_OFFSET_CAP = 200_000               # safety: a single month exceeding this
EQ_MONTHS_BACK = 24                     # bounded snapshot horizon (months)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _month_windows(now: datetime, months_back: int):
    """Yield (label, start, end) calendar-month windows, oldest first, covering
    the trailing `months_back` months up to `now` (the final window is partial)."""
    y, m = now.year, now.month
    starts = []
    for _ in range(months_back):
        starts.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    starts.reverse()
    for i, (yy, mm) in enumerate(starts):
        start = datetime(yy, mm, 1, tzinfo=timezone.utc)
        if i + 1 < len(starts):
            ny, nm = starts[i + 1]
            end = datetime(ny, nm, 1, tzinfo=timezone.utc)
        else:
            end = now
        yield f"{yy:04d}-{mm:02d}", start, end


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


def fetch_earthquakes(node_id: str) -> None:
    asset = node_id
    now = datetime.now(timezone.utc).replace(microsecond=0)
    total = 0
    for label, start, end in _month_windows(now, EQ_MONTHS_BACK):
        rows = _query_events(start, end)
        if rows:
            save_raw_ndjson(rows, f"{asset}-{label}")   # one batch per month
            total += len(rows)
        print(f"[{asset}] {label}: {len(rows)} events ({total} total)", flush=True)
    if total == 0:
        raise RuntimeError(f"{asset}: FDSN returned no events for the snapshot window")
    print(f"[{asset}] done: {total} events", flush=True)


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
            PARTITION BY field_measurements_series_id, parameter_code, "time"
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
