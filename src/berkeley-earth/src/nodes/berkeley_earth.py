"""Berkeley Earth — download step.

Two collect entities, two access shapes (both from research's chosen
`bulk_global_timeseries` family on the anonymous public buckets; the per-station
detail comes from the sibling `bulk_station_archives` ZIPs that research
documented for exactly this case):

  1. regional-temperature-series
     Headline standard product on the anonymous S3 bucket
     (berkeley-earth-temperature.s3.us-west-1.amazonaws.com). A fixed set of
     whitespace-delimited text files: global land+ocean, global land (TAVG),
     and the regional/hemispheric/continental "Trend" series for all three
     variables (TAVG/TMAX/TMIN) across 10 regions = 32 files. Each is a few
     hundred KB. Probed live: all 32 return 200/206. Files are overwritten in
     place at the same URL each refresh, so we just re-download every run.

  2. station-observations
     Quality Controlled station archives on the anonymous GCS bucket
     (storage.googleapis.com/berkeley-earth-stations/Berkeley-Earth/<VAR>/
     Monthly/LATEST - Quality Controlled.zip) — one ZIP per variable bundling
     all stations' long-format monthly observations. Probed live: TAVG 296 MB,
     TMIN 240 MB, TMAX 240 MB (~777 MB total), content-type application/zip,
     Range supported (206 + accept-ranges), each with an ETag. We stream each
     ZIP in fixed byte chunks, advancing a per-file byte watermark in state
     after every chunk, bounded by a per-run wall-clock budget. Memory stays
     bounded (one chunk at a time — never the whole ZIP) and a crash/budget
     stop loses at most the in-flight chunk. ETag is the version key: if a
     file's ETag changes between runs, that file restarts from byte 0.

License: CC BY-NC 4.0 (non-commercial) — recorded for downstream curation.
"""

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
    get,
    save_raw_file,
    raw_writer,
    load_state,
    save_state,
)

# Bump when the shape of persisted station state changes (keys / watermark
# representation). Forces the next dev run to discard incompatible state.
STATE_VERSION = 2

# --- hosts -------------------------------------------------------------------
S3 = "https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com"
GCS_STATIONS = "https://storage.googleapis.com/berkeley-earth-stations"

# --- regional product: fixed file set (all verified live) --------------------
# (url, batch_key) — batch_key is pure file/region info, no slug/entity name.
_GLOBAL_FILES = [
    (f"{S3}/Global/Land_and_Ocean_complete.txt", "global-land-ocean-complete"),
    (f"{S3}/Global/Complete_TAVG_complete.txt", "global-land-complete-tavg"),
]
_REGIONS = [
    "global-land", "northern-hemisphere", "southern-hemisphere",
    "africa", "asia", "europe", "north-america", "south-america",
    "oceania", "antarctica",
]
_REGIONAL_VARS = ("TAVG", "TMAX", "TMIN")
_REGIONAL_FILES = _GLOBAL_FILES + [
    (f"{S3}/Regional/{v}/{r}-{v}-Trend.txt", f"regional-{v.lower()}-{r}")
    for v in _REGIONAL_VARS
    for r in _REGIONS
]

# --- station archives: Quality Controlled ZIP per variable (large; streamed) -
# (url, batch_key, extension)
_STATION_VARS = ("TAVG", "TMIN", "TMAX")
_STATION_FILES = [
    (f"{GCS_STATIONS}/Berkeley-Earth/{v}/Monthly/LATEST%20-%20Quality%20Controlled.zip",
     f"{v.lower()}-quality-controlled", "zip")
    for v in _STATION_VARS
]

# Streaming knobs. Peak RAM per spec ~ one chunk in the httpx response buffer
# plus the write buffer, so a small chunk keeps us clear of the OOM that
# loading a multi-hundred-MB ZIP whole would cause. The wall-clock budget
# returns the run cleanly (state saved) so a long backfill spans refreshes
# instead of risking a timeout-kill that orphans the spec.
_CHUNK = 64 * 1024 * 1024           # 64 MB per Range request — bounds RAM
_MAX_FETCH_SECONDS = 600            # soft per-run budget (~10 min); resumes next run
_RANGE_UNSUPPORTED_MAX = 400 * 1024 * 1024  # refuse to buffer a non-range file bigger than this


# =============================================================================
# Retry / transport
# =============================================================================

_TRANSIENT_EXC = (
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
    httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError,
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
def _http_get(url: str, headers: dict | None = None) -> httpx.Response:
    """GET with retry on transient failures. Raises on non-2xx — 4xx propagate
    immediately (predicate rejects them); 5xx/429 are retried with backoff."""
    resp = get(url, headers=headers, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp


def _is_permanent_status(exc: BaseException) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code != 429
        and 400 <= exc.response.status_code < 500
    )


# =============================================================================
# Entity 1: regional temperature series (small fixed files, overwrite each run)
# =============================================================================

def fetch_regional_series(entity_id: str) -> None:
    asset_base = f"berkeley-earth-{entity_id}"
    fetched, total_bytes = 0, 0
    for url, key in _REGIONAL_FILES:
        asset = f"{asset_base}-{key}"
        try:
            resp = _http_get(url)
        except Exception as exc:  # noqa: BLE001 - logged with url + class
            if _is_permanent_status(exc):
                code = exc.response.status_code
                print(f"  [skip] {url} permanent HTTP {code}", flush=True)
                continue
            print(f"  [error] {url}: {type(exc).__name__}: {exc}", flush=True)
            raise
        content = resp.content
        save_raw_file(content, asset, extension="txt")
        fetched += 1
        total_bytes += len(content)
    if fetched == 0:
        raise RuntimeError("regional-temperature-series: fetched 0 files — "
                           "all regional URLs failed; source layout may have changed")
    print(f"  regional-temperature-series: {fetched}/{len(_REGIONAL_FILES)} files, "
          f"{total_bytes:,} bytes", flush=True)


# =============================================================================
# Entity 2: station observations (Quality Controlled ZIPs, streamed)
# =============================================================================

def _probe_meta(url: str) -> dict:
    """Return {'size': int, 'etag': str|None, 'range': bool} via a 1-byte Range
    probe. Falls back to the full content length if Range is unsupported."""
    resp = _http_get(url, headers={"Range": "bytes=0-0"})
    etag = resp.headers.get("etag")
    if resp.status_code == 206:
        cr = resp.headers.get("content-range", "")
        total = int(cr.split("/")[-1]) if "/" in cr else 0
        return {"size": total, "etag": etag, "range": True}
    clen = resp.headers.get("content-length")
    size = int(clen) if clen else len(resp.content)
    return {"size": size, "etag": etag, "range": False}


def fetch_station_observations(entity_id: str) -> None:
    asset_base = f"berkeley-earth-{entity_id}"
    state_key = asset_base
    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        if state:
            print(f"  [state] resetting (schema {state.get('schema_version')} "
                  f"!= {STATE_VERSION})", flush=True)
        state = {"schema_version": STATE_VERSION, "files": {}}
    files_state = state.setdefault("files", {})

    run_start = time.monotonic()
    run_bytes = 0

    for url, key, ext in _STATION_FILES:
        asset = f"{asset_base}-{key}"
        try:
            meta = _probe_meta(url)
        except Exception as exc:  # noqa: BLE001
            if _is_permanent_status(exc):
                code = exc.response.status_code
                print(f"  [skip] {url} permanent HTTP {code}", flush=True)
                files_state[key] = {"etag": None, "size": 0, "bytes": 0,
                                    "skipped": f"HTTP {code}"}
                save_state(state_key, state)
                continue
            print(f"  [error] probe {url}: {type(exc).__name__}: {exc}", flush=True)
            raise

        total = meta["size"]
        fs = files_state.get(key, {})

        # Already have the current version in full? skip (idempotent).
        if fs.get("etag") == meta["etag"] and fs.get("bytes", 0) >= total and total > 0:
            continue

        # Resume offset: same ETag + range support → continue; else restart.
        if fs.get("etag") == meta["etag"] and meta["range"] and fs.get("bytes", 0) > 0:
            pos = fs["bytes"]
        else:
            pos = 0

        if not meta["range"]:
            if total > _RANGE_UNSUPPORTED_MAX:
                raise RuntimeError(
                    f"{url}: server does not support Range and file is "
                    f"{total:,} bytes — refusing to buffer in memory")
            resp = _http_get(url)
            save_raw_file(resp.content, asset, extension=ext)
            files_state[key] = {"etag": meta["etag"], "size": len(resp.content),
                                "bytes": len(resp.content)}
            save_state(state_key, state)
            run_bytes += len(resp.content)
            continue

        print(f"  [stream] {key}: {pos:,}/{total:,} bytes"
              + (" (resume)" if pos else ""), flush=True)

        while pos < total:
            if time.monotonic() - run_start > _MAX_FETCH_SECONDS:
                print(f"  [budget] stopping at {key} {pos:,}/{total:,}; "
                      f"resumes next run", flush=True)
                state["last_run_stats"] = {"bytes": run_bytes,
                                           "stopped_at": key, "pos": pos}
                save_state(state_key, state)
                return

            end = min(pos + _CHUNK - 1, total - 1)
            resp = _http_get(url, headers={"Range": f"bytes={pos}-{end}"})
            data = resp.content
            if not data:
                print(f"  [warn] empty chunk at {key} pos={pos:,}; stopping file",
                      flush=True)
                break

            # Write raw BEFORE advancing state. Append for every chunk after the
            # first byte; truncate (wb) only when starting a file from zero.
            with raw_writer(asset, extension=ext, mode=("ab" if pos else "wb")) as f:
                f.write(data)
            pos += len(data)
            run_bytes += len(data)
            files_state[key] = {"etag": meta["etag"], "size": total, "bytes": pos}
            save_state(state_key, state)
            print(f"    {key}: {pos:,}/{total:,} ({100 * pos // total}%)", flush=True)

    state["last_run_stats"] = {"bytes": run_bytes, "stopped_at": None}
    save_state(state_key, state)
    print(f"  station-observations: {run_bytes:,} bytes this run", flush=True)


# =============================================================================
# Specs — one per entity in the union
# =============================================================================

DOWNLOAD_SPECS = [
    NodeSpec(
        id="berkeley-earth-regional-temperature-series",
        fn=fetch_regional_series,
        args=("regional-temperature-series",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="berkeley-earth-station-observations",
        fn=fetch_station_observations,
        args=("station-observations",),
        deps=(),
        kind="download",
    ),
]
