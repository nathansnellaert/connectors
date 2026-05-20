"""Berkeley Earth — download step.

Two collect entities, two NodeSpecs:

  * regional-temperature-series — the headline Berkeley Earth product:
    pre-aggregated TAVG anomaly time series for the globe (land+ocean and
    land-only), the two hemispheres, and the seven continents. ~11 small
    whitespace-delimited text files, each with a long %-prefixed header
    block. Fetched into one combined JSON raw asset.

  * station-observations — per-station monthly observations, taken from
    the Berkeley Earth *Quality Controlled* station archive ZIPs (the
    handoff explicitly routes per-station detail to bulk_station_archives).
    One ~240-300 MB ZIP per variable (TAVG/TMIN/TMAX), streamed to disk.

No incremental query is possible: Berkeley Earth overwrites files in place
at stable URLs — there is no date/cursor filter. Idempotency is therefore
a freshness short-circuit on the raw asset, sized to the source's publish
cadence. License: CC BY-NC 4.0 (non-commercial).
"""

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from subsets_utils import (
    NodeSpec, get, get_client, save_raw_json, raw_asset_exists, raw_writer, save_state,
)

# --- fetch surface -----------------------------------------------------------

# Global series: standard product, anonymous public S3 (us-west-1).
GLOBAL_FILES = {
    "global-land-and-ocean": "https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com/Global/Land_and_Ocean_complete.txt",
    "global-land": "https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com/Global/Complete_TAVG_complete.txt",
}

# Hemispheric + continental land-surface TAVG series, anonymous public host.
# Slugs verified live against data.berkeleyearth.org during authoring.
REGIONAL_BASE = "https://data.berkeleyearth.org/auto/Regional/TAVG/Text/"
REGIONAL_SLUGS = [
    "northern-hemisphere",
    "southern-hemisphere",
    "africa",
    "antarctica",
    "asia",
    "australia",
    "europe",
    "north-america",
    "south-america",
]

# Quality Controlled station archives — one ZIP per variable, anonymous GCS.
STATION_VARIABLES = ["TAVG", "TMIN", "TMAX"]
STATION_URL = (
    "https://storage.googleapis.com/berkeley-earth-stations/Berkeley-Earth/"
    "{var}/Monthly/LATEST%20-%20Quality%20Controlled.zip"
)


# --- retry / transport -------------------------------------------------------

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
def _fetch_text(url: str) -> str:
    """Fetch a small whitespace-delimited text series file."""
    resp = get(url, timeout=(10.0, 60.0))  # (connect, read)
    resp.raise_for_status()
    return resp.text


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _stream_zip(url: str, asset_id: str) -> None:
    """Stream a large station-archive ZIP straight to disk (memory-bounded)."""
    client = get_client()
    written = 0
    next_log = 50 * 1024 * 1024
    with client.stream("GET", url, timeout=(15.0, 300.0)) as resp:
        resp.raise_for_status()
        with raw_writer(asset_id, "zip", mode="wb") as out:
            for chunk in resp.iter_bytes(chunk_size=4 * 1024 * 1024):
                out.write(chunk)
                written += len(chunk)
                if written >= next_log:
                    print(f"  {asset_id}: {written // (1024 * 1024)} MB streamed", flush=True)
                    next_log += 50 * 1024 * 1024
    if written < 1024 * 1024:
        raise AssertionError(f"{asset_id}: archive truncated, only {written} bytes")
    print(f"  {asset_id}: done, {written // (1024 * 1024)} MB", flush=True)


# --- fetch functions ---------------------------------------------------------

def fetch_regional_series(entity_id: str) -> None:
    """Download every global/hemispheric/continental TAVG series into one
    combined JSON raw asset keyed by region."""
    asset = f"berkeley-earth-{entity_id}"
    # Berkeley Earth refreshes the standard global/regional product monthly
    # (per https://berkeleyearth.org/data/); 25d window catches each release.
    if raw_asset_exists(asset, ext="json", max_age_days=25):
        return

    series: dict = {}
    for key, url in GLOBAL_FILES.items():
        series[key] = {"url": url, "text": _fetch_text(url)}
    for slug in REGIONAL_SLUGS:
        url = f"{REGIONAL_BASE}{slug}-TAVG-Trend.txt"
        series[slug] = {"url": url, "text": _fetch_text(url)}

    save_raw_json(series, asset)  # raw before state, always
    total_bytes = sum(len(v["text"]) for v in series.values())
    save_state(asset, {
        "schema_version": 1,
        "last_run_stats": {"files": len(series), "bytes": total_bytes},
    })


def fetch_station_observations(entity_id: str) -> None:
    """Download the Quality Controlled station archive ZIP for each
    temperature variable. Each variable is its own raw asset (one ~240-300 MB
    ZIP); a fresh archive short-circuits per-variable."""
    base = f"berkeley-earth-{entity_id}"
    fetched = []
    for var in STATION_VARIABLES:
        asset = f"{base}-{var.lower()}"
        # Berkeley Earth station archives refresh on the order of months
        # (per https://berkeleyearth.org/source-files/); 45d freshness window.
        if raw_asset_exists(asset, ext="zip", max_age_days=45):
            continue
        url = STATION_URL.format(var=var)
        _stream_zip(url, asset)  # raw written to disk inside this call
        fetched.append(var)

    save_state(base, {
        "schema_version": 1,
        "last_run_stats": {"variables": STATION_VARIABLES, "fetched_this_run": fetched},
    })


# --- specs -------------------------------------------------------------------

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
