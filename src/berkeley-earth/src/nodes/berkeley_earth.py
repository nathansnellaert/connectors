"""Berkeley Earth — download step.

Two collect entities, two fetch shapes (both stateless full re-pull — the whole
corpus is small enough to re-fetch every refresh; the standard product is
overwritten in place at stable URLs and revisions are picked up for free):

  * ``regional-temperature-series`` — the headline pre-aggregated anomaly text
    series for the globe, the two hemispheres and the seven continents. Each
    file is a ~100-line ``%``-prefixed header followed by 12 whitespace-delimited
    columns (year, month, monthly anomaly+uncertainty, then 1/5/10/20-year
    moving averages each with an uncertainty). Layout is identical across every
    file, so we parse all of them into ONE long-format parquet with dimension
    columns (region / surface / variable / sea-ice method). Files live on the
    standard S3 product bucket (us-west-1, anonymous). Total payload is a few MB.

  * ``station-observations`` — per-station monthly observations. The raw form is
    the Quality Controlled station archive ZIP, one per variable (TAVG/TMIN/TMAX,
    ~240-300 MB each). We save each archive verbatim as opaque bytes; the
    transform step unzips and parses the tens of thousands of bundled per-station
    files. Three archives are written under one spec using batch-key asset ids
    (``...-station-observations-<VAR>``); transform enumerates them with
    ``list_raw_files``.

License: CC BY-NC 4.0 (non-commercial) — recorded for downstream curation.
"""

import math

import httpx
import pyarrow as pa
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_parquet, save_raw_file

# ---------------------------------------------------------------------------
# Transport / retry
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
def _fetch_text(url: str) -> str:
    resp = get(url, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp.text


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_bytes(url: str) -> bytes:
    # Read timeout generous — station archives are 240-300 MB.
    resp = get(url, timeout=(10.0, 600.0))
    resp.raise_for_status()
    return resp.content


# ---------------------------------------------------------------------------
# Entity 1: regional-temperature-series
# ---------------------------------------------------------------------------

_S3 = "https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com"

# (region, surface, variable, url) — every URL verified live during probing.
# surface "land_ocean" is the combined land+ocean product (two sections in the
# file: air-temperature-over-sea-ice then water-temperature-over-sea-ice);
# "land" is the land-only product (single section).
_SERIES_FILES = [
    # Global headline products
    ("global", "land_ocean", "TAVG", f"{_S3}/Global/Land_and_Ocean_complete.txt"),
    ("global", "land", "TAVG", f"{_S3}/Global/Complete_TAVG_complete.txt"),
    ("global", "land", "TMAX", f"{_S3}/Global/Complete_TMAX_complete.txt"),
    ("global", "land", "TMIN", f"{_S3}/Global/Complete_TMIN_complete.txt"),
    # Hemispheres + seven continents (land-only TAVG regional trend files)
    ("northern-hemisphere", "land", "TAVG", f"{_S3}/Regional/TAVG/northern-hemisphere-TAVG-Trend.txt"),
    ("southern-hemisphere", "land", "TAVG", f"{_S3}/Regional/TAVG/southern-hemisphere-TAVG-Trend.txt"),
    ("africa", "land", "TAVG", f"{_S3}/Regional/TAVG/africa-TAVG-Trend.txt"),
    ("antarctica", "land", "TAVG", f"{_S3}/Regional/TAVG/antarctica-TAVG-Trend.txt"),
    ("asia", "land", "TAVG", f"{_S3}/Regional/TAVG/asia-TAVG-Trend.txt"),
    ("australia", "land", "TAVG", f"{_S3}/Regional/TAVG/australia-TAVG-Trend.txt"),
    ("europe", "land", "TAVG", f"{_S3}/Regional/TAVG/europe-TAVG-Trend.txt"),
    ("north-america", "land", "TAVG", f"{_S3}/Regional/TAVG/north-america-TAVG-Trend.txt"),
    ("south-america", "land", "TAVG", f"{_S3}/Regional/TAVG/south-america-TAVG-Trend.txt"),
]

_SERIES_SCHEMA = pa.schema([
    ("region", pa.string()),
    ("surface", pa.string()),
    ("variable", pa.string()),
    # For land_ocean files only: "air" (sea-ice anomalies from air temps) or
    # "water" (from sea-surface temps). None for single-section land files.
    ("sea_ice_method", pa.string()),
    ("frequency", pa.string()),
    ("year", pa.int32()),
    ("month", pa.int16()),
    ("monthly_anomaly", pa.float64()),
    ("monthly_unc", pa.float64()),
    ("annual_anomaly", pa.float64()),
    ("annual_unc", pa.float64()),
    ("five_year_anomaly", pa.float64()),
    ("five_year_unc", pa.float64()),
    ("ten_year_anomaly", pa.float64()),
    ("ten_year_unc", pa.float64()),
    ("twenty_year_anomaly", pa.float64()),
    ("twenty_year_unc", pa.float64()),
    ("source_url", pa.string()),
])

# Section index -> sea-ice method, for the combined land+ocean product.
_SEA_ICE_METHODS = ("air", "water")


def _f(token: str):
    """Parse one numeric token; Berkeley Earth uses 'NaN' for missing."""
    if token == "NaN":
        return None
    val = float(token)
    return None if math.isnan(val) else val


def _parse_series(text: str, region: str, surface: str, variable: str, url: str) -> list[dict]:
    """Parse one Berkeley Earth trend file into long-format rows.

    Data lines are 12 whitespace-delimited columns. A file may contain more than
    one section (the land+ocean product reports air- then water-over-sea-ice);
    sections are delimited by the year counter resetting downward.
    """
    rows: list[dict] = []
    section = 0
    prev_year = None
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("%"):
            continue
        parts = s.split()
        if len(parts) != 12:
            raise ValueError(f"{url}: expected 12 columns, got {len(parts)}: {s!r}")
        year = int(parts[0])
        month = int(parts[1])
        if prev_year is not None and year < prev_year:
            section += 1
        prev_year = year
        sea_ice = _SEA_ICE_METHODS[section] if surface == "land_ocean" else None
        if surface == "land_ocean" and section >= len(_SEA_ICE_METHODS):
            raise ValueError(f"{url}: unexpected extra section {section}")
        rows.append({
            "region": region,
            "surface": surface,
            "variable": variable,
            "sea_ice_method": sea_ice,
            "frequency": "monthly",
            "year": year,
            "month": month,
            "monthly_anomaly": _f(parts[2]),
            "monthly_unc": _f(parts[3]),
            "annual_anomaly": _f(parts[4]),
            "annual_unc": _f(parts[5]),
            "five_year_anomaly": _f(parts[6]),
            "five_year_unc": _f(parts[7]),
            "ten_year_anomaly": _f(parts[8]),
            "ten_year_unc": _f(parts[9]),
            "twenty_year_anomaly": _f(parts[10]),
            "twenty_year_unc": _f(parts[11]),
            "source_url": url,
        })
    return rows


def fetch_regional_series(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    rows: list[dict] = []
    for region, surface, variable, url in _SERIES_FILES:
        text = _fetch_text(url)
        parsed = _parse_series(text, region, surface, variable, url)
        if not parsed:
            raise ValueError(f"{url}: parsed 0 data rows")
        rows.extend(parsed)
        print(f"[{asset}] {region}/{surface}/{variable}: {len(parsed)} rows", flush=True)
    table = pa.Table.from_pylist(rows, schema=_SERIES_SCHEMA)
    save_raw_parquet(table, asset)
    print(f"[{asset}] total {len(rows)} rows across {len(_SERIES_FILES)} files", flush=True)


# ---------------------------------------------------------------------------
# Entity 2: station-observations
# ---------------------------------------------------------------------------

_STATIONS = "https://storage.googleapis.com/berkeley-earth-stations/Berkeley-Earth"
# Quality Controlled monthly archives — one per variable. Prefer QC over the
# raw Multi/Single-valued variants (handoff guidance). %20 == space in the path.
_STATION_VARIABLES = ("TAVG", "TMIN", "TMAX")


def _station_archive_url(variable: str) -> str:
    return f"{_STATIONS}/{variable}/Monthly/LATEST%20-%20Quality%20Controlled.zip"


def fetch_station_observations(node_id: str) -> None:
    # One spec, three large opaque archives — written under batch-key asset ids
    # so they coexist; transform discovers them via list_raw_files(prefix=...).
    for variable in _STATION_VARIABLES:
        url = _station_archive_url(variable)
        content = _fetch_bytes(url)
        if content[:4] != b"PK\x03\x04":
            raise ValueError(f"{url}: not a ZIP archive (got {content[:4]!r})")
        asset = f"{node_id}-{variable}"
        save_raw_file(content, asset, extension="zip")
        print(f"[{node_id}] {variable}: {len(content)} bytes -> {asset}.zip", flush=True)


# ---------------------------------------------------------------------------
# Specs — one per entity in the entity union
# ---------------------------------------------------------------------------

DOWNLOAD_SPECS = [
    NodeSpec(id="berkeley-earth-regional-temperature-series", fn=fetch_regional_series, kind="download"),
    NodeSpec(id="berkeley-earth-station-observations", fn=fetch_station_observations, kind="download"),
]
