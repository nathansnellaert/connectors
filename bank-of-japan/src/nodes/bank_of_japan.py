"""Bank of Japan — download step.

Source: BoJ Time-Series Data Search REST API (rest_v1), base
https://www.stat-search.boj.or.jp/api/v1/ . Public, no auth.

One DOWNLOAD_SPEC per BoJ database (entity). Per-database strategy, exactly
as research's download_handoff prescribes:

  1. GET /getMetadata?db=<DB>&format=json&lang=en — enumerate every series
     code + frequency + per-series LAST_UPDATE.
  2. Group series codes by frequency (a /getDataCode request must not mix
     frequencies) and batch each group through /getDataCode in chunks of
     250 codes (the documented per-request code cap).
  3. Within a chunk, follow the NEXTPOSITION cursor (via the startPosition
     param) until the page sequence ends — pagination fires whenever the
     60,000-data-point or 250-code cap is hit.

Incremental contract: the API exposes no since/modifiedAfter filter, so the
documented contract is a full-corpus re-pull. We make that idempotent with a
client-side watermark = max(LAST_UPDATE) across the database's series. If the
watermark is unchanged from the previous run and the raw data file is already
on disk, the (expensive) data crawl is skipped. The watermark advances
monotonically as the source publishes — it is never a terminal flag.

Raw layout per spec id `bank-of-japan-<db>`:
  - <sid>.ndjson.gz       — one JSON line per /getDataCode response page,
                            each wrapped with request context (db, frequency,
                            code chunk, page index).
  - <sid>-metadata.json.gz — the full /getMetadata response.
"""

import json
from datetime import datetime, timezone

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
    load_state,
    raw_asset_exists,
    raw_writer,
    save_raw_json,
    save_state,
)

BASE_URL = "https://www.stat-search.boj.or.jp/api/v1/"

# Documented per-request code cap for /getDataCode.
CHUNK_SIZE = 250
# Safety cap on cursor pages within a single 250-code chunk. With the 60,000
# data-point cap, even a chunk of very long daily series resolves in a handful
# of pages; hitting this many means the cursor is not terminating — raise.
MAX_PAGES_PER_CHUNK = 200

# The BoJ manual documents no numeric rate limit, only a warning that
# "excessive access frequency may result in a restriction of access". The
# research handoff advises throttling to <=2 req/s; we run at ~1.5 req/s
# (~80% of that ceiling). NodeSpecs run sequentially by default
# (DAG_PARALLELISM=1), so this per-process limiter governs the whole crawl.
_RL_CALLS = 3
_RL_PERIOD = 2  # seconds -> 1.5 req/s

# The 46 BoJ databases scored at/above the publish threshold — the entity
# union. Copied verbatim from the entity-union file; each is a DB code passed
# straight to the API's `db` parameter.
ENTITY_IDS = [
    "BIS", "BP01", "BS01", "BS02", "CO", "DER", "FF",
    "FM01", "FM02", "FM03", "FM04", "FM05", "FM06", "FM08", "FM09",
    "IR01", "IR02", "IR03", "IR04",
    "LA01", "LA02", "LA03", "LA04", "LA05",
    "MD01", "MD02", "MD03", "MD05", "MD06", "MD07", "MD08", "MD09",
    "MD10", "MD11", "MD12", "MD13", "MD14",
    "OB01", "OB02",
    "PF01", "PF02",
    "PR01", "PR02", "PR04",
    "PS01", "PS02",
]


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

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
@limits(calls=_RL_CALLS, period=_RL_PERIOD)
def _throttle() -> None:
    """Process-wide pacing gate — one tick per API call."""
    return None


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _api_get(endpoint: str, params: dict) -> dict:
    """One GET against the BoJ API. JSON in, parsed dict/list out.

    Transient failures (429, 5xx, connection/read timeouts) are retried with
    exponential backoff; everything else propagates.
    """
    _throttle()
    url = BASE_URL + endpoint
    resp = get(url, params=params, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp.json()


# --------------------------------------------------------------------------
# Response parsing helpers — the BoJ API publishes no JSON schema, so these
# locate fields defensively (case-insensitive, several known aliases).
# --------------------------------------------------------------------------


def _ci_get(record: dict, *names: str):
    """Case-insensitive lookup of the first matching key in `record`."""
    lowered = {str(k).lower(): v for k, v in record.items()}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def _api_error_message(payload):
    """Return an explicit API error message if the payload is an error doc.

    Errors are always returned as JSON regardless of the requested format.
    Kept conservative — only an `errorMessage`-style key counts, so success
    payloads carrying an `errorCode: "0"` field are not misread as failures.
    """
    if isinstance(payload, dict):
        for key, value in payload.items():
            kl = str(key).lower().replace("_", "")
            if kl in ("errormessage", "errordetail") and value:
                return str(value)
    return None


def _find_series_records(meta) -> list:
    """Locate the list of per-series metadata records inside a /getMetadata
    response. Picks the longest list-of-dicts found anywhere in the doc."""
    if isinstance(meta, list):
        return [x for x in meta if isinstance(x, dict)]
    best: list = []
    if isinstance(meta, dict):
        for value in meta.values():
            if isinstance(value, list):
                dicts = [x for x in value if isinstance(x, dict)]
                if len(dicts) > len(best):
                    best = dicts
            elif isinstance(value, dict):
                inner = _find_series_records(value)
                if len(inner) > len(best):
                    best = inner
    return best


def _extract_next_position(payload):
    """Find a NEXTPOSITION cursor value anywhere in a response. Returns None
    when absent or falsy ("", 0, "0") — i.e. the page sequence is complete."""
    if isinstance(payload, dict):
        for key, value in payload.items():
            if str(key).lower().replace("_", "") == "nextposition":
                if value not in (None, "", 0, "0"):
                    return value
        for value in payload.values():
            found = _extract_next_position(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _extract_next_position(value)
            if found is not None:
                return found
    return None


def _chunked(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# --------------------------------------------------------------------------
# Fetch
# --------------------------------------------------------------------------


def _fetch_metadata(db: str) -> dict:
    """GET /getMetadata for one database. Raises on an explicit API error."""
    payload = _api_get(
        "getMetadata",
        {"format": "json", "lang": "en", "db": db},
    )
    err = _api_error_message(payload)
    if err:
        raise RuntimeError(f"getMetadata[{db}] returned API error: {err}")
    return payload


def _series_by_frequency(records: list) -> dict:
    """Group series codes by frequency. Series codes are DB-scoped and passed
    bare — never prefix the DB name (the API rejects qualified codes)."""
    groups: dict = {}
    for rec in records:
        code = _ci_get(rec, "code", "series_code", "seriescode", "tsCode")
        if not code:
            continue
        freq = _ci_get(rec, "frequency", "freq") or "UNKNOWN"
        groups.setdefault(str(freq), []).append(str(code))
    return groups


def _watermark(records: list) -> str:
    """Max LAST_UPDATE (YYYYMMDD) across the database — the refresh watermark."""
    stamps = []
    for rec in records:
        lu = _ci_get(rec, "last_update", "lastupdate", "lastUpdated")
        if lu:
            stamps.append(str(lu))
    return max(stamps) if stamps else ""


def _fetch_data(sid: str, db: str, groups: dict) -> tuple:
    """Crawl every series of one database through /getDataCode, streaming each
    response page as one JSON line into <sid>.ndjson.gz.

    Returns (pages_written, bytes_written)."""
    total_chunks = sum(
        (len(codes) + CHUNK_SIZE - 1) // CHUNK_SIZE for codes in groups.values()
    )
    pages = 0
    nbytes = 0
    done_chunks = 0

    with raw_writer(sid, "ndjson.gz", mode="wt", compression="gzip") as out:
        for freq, codes in sorted(groups.items()):
            for chunk_index, chunk in enumerate(_chunked(codes, CHUNK_SIZE)):
                start_position = None
                for page in range(MAX_PAGES_PER_CHUNK):
                    params = {
                        "format": "json",
                        "lang": "en",
                        "db": db,
                        "code": ",".join(chunk),
                    }
                    if start_position is not None:
                        params["startPosition"] = start_position
                    payload = _api_get("getDataCode", params)
                    err = _api_error_message(payload)
                    if err:
                        raise RuntimeError(
                            f"getDataCode[{db}/{freq} chunk {chunk_index}] "
                            f"API error: {err}"
                        )
                    record = {
                        "db": db,
                        "endpoint": "getDataCode",
                        "frequency": freq,
                        "chunk_index": chunk_index,
                        "page": page,
                        "codes": chunk,
                        "response": payload,
                    }
                    line = json.dumps(record, ensure_ascii=False) + "\n"
                    out.write(line)
                    pages += 1
                    nbytes += len(line.encode("utf-8"))

                    start_position = _extract_next_position(payload)
                    if start_position is None:
                        break
                else:
                    raise RuntimeError(
                        f"{sid}: chunk {chunk_index} of frequency {freq} did "
                        f"not terminate within {MAX_PAGES_PER_CHUNK} cursor "
                        f"pages — NEXTPOSITION may not be advancing"
                    )

                done_chunks += 1
                if done_chunks % 25 == 0 or done_chunks == total_chunks:
                    print(
                        f"  [{sid}] {done_chunks}/{total_chunks} code-chunks, "
                        f"{pages} pages, {nbytes // 1024} KiB",
                        flush=True,
                    )

    return pages, nbytes


# --------------------------------------------------------------------------
# Entity fetch fn
# --------------------------------------------------------------------------


def fetch_one(entity_id: str) -> None:
    """Download one BoJ database: enumerate its series, then crawl their data.

    Idempotent across refreshes via a max(LAST_UPDATE) watermark — when the
    database has not been republished since the last successful run and the
    raw data file is still present, the data crawl is skipped.
    """
    db = entity_id
    sid = f"bank-of-japan-{entity_id.lower().replace('_', '-')}"

    state = load_state(sid)
    if state.get("schema_version") != 1:
        if state:
            print(f"  [{sid}] resetting state (unknown schema_version)", flush=True)
        state = {}

    # Metadata is the source of truth for what to fetch — always refreshed.
    meta = _fetch_metadata(db)
    records = _find_series_records(meta)
    print(f"  [{sid}] metadata: {len(records)} series", flush=True)

    watermark = _watermark(records)
    data_present = raw_asset_exists(sid, ext="ndjson.gz")

    # Raw is written before state, always: save metadata first.
    save_raw_json(meta, f"{sid}-metadata", compress=True)

    if watermark and data_present and state.get("watermark") == watermark:
        print(
            f"  [{sid}] watermark unchanged ({watermark}) and data present "
            f"— skipping data crawl",
            flush=True,
        )
        return

    if not records:
        raise RuntimeError(f"{sid}: /getMetadata returned no series records")

    groups = _series_by_frequency(records)
    pages, nbytes = _fetch_data(sid, db, groups)

    if pages == 0:
        raise RuntimeError(f"{sid}: /getDataCode crawl produced no pages")

    # State written only after the raw data file is fully on disk.
    save_state(sid, {
        "schema_version": 1,
        "watermark": watermark,
        "last_success_at": datetime.now(tz=timezone.utc).isoformat(),
        "last_run_stats": {
            "series": len(records),
            "frequencies": {f: len(c) for f, c in groups.items()},
            "pages": pages,
            "bytes": nbytes,
        },
    })


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bank-of-japan-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        args=(eid,),
        deps=(),
        kind="download",
    )
    for eid in ENTITY_IDS
]
