"""Berkeley Earth connector — implement step.

Two published subsets, one download spec each:

  * regional-temperature-series — headline pre-aggregated temperature
    series for the globe, the two hemispheres and the eight continental
    regions, land+ocean and land-only, for TAVG/TMAX/TMIN. Long format:
    region/surface/variable are dimension *columns*, not separate schemas.
    Sourced from the stable standard product on S3 (global complete files,
    full moving-average layout — we keep the monthly anomaly + uncertainty)
    plus the high-resolution beta product on GCS (continent / hemisphere
    monthly text series). All are small whitespace-delimited text files with
    a long %-prefixed header. The global "Land_and_Ocean" / "Complete_*"
    files contain TWO data sections (air- vs water-temperature over sea ice);
    we take the first (air-temperature, the Berkeley-recommended version) by
    stopping at the year/month restart.

  * station-observations — per-station monthly observations across all
    stations and all three variables, from the "Quality Controlled" station
    archives (one ~240-300 MB ZIP per variable). Each ZIP holds a single
    consolidated `data.txt` (~900 MB uncompressed, tab-delimited:
    station id, series number, decimal date, temperature degC, uncertainty,
    observation count, time-of-obs). We range-fetch only the compressed
    `data.txt` member, stream-inflate it, and write batched parquet — never
    holding the full file in memory. ~50M rows across the three variables.
    Per-datum QC flags (flags.txt, ~1.3 GB) are intentionally omitted.

Full corpus per refresh — every URL is a stable "LATEST"/bare-name file that
is overwritten in place, so we re-pull and overwrite (freshness gating is the
maintain step's job). License: CC BY-NC 4.0 (non-commercial).
"""
import math
import struct
import zlib

import httpx
import pyarrow as pa
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
    raw_parquet_writer,
)

# ---------------------------------------------------------------------------
# HTTP transport — retried, timed-out, honest about transient vs permanent.
# ---------------------------------------------------------------------------

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
def _http(url: str, *, headers: dict | None = None) -> httpx.Response:
    # Large members stream slowly; allow a generous read timeout.
    resp = get(url, headers=headers, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp


# ===========================================================================
# Entity 1: regional-temperature-series
# ===========================================================================

_S3_GLOBAL = "https://berkeley-earth-temperature.s3.us-west-1.amazonaws.com/Global/{fname}"
_HR = "https://storage.googleapis.com/berkeley-earth-temperature-hr/{path}"

# (region, surface, variable, url). Stable standard product on S3 — full
# moving-average column layout; we read only the monthly anomaly + uncertainty.
_GLOBAL_SERIES = [
    ("global", "land_and_ocean", "TAVG",
     _S3_GLOBAL.format(fname="Land_and_Ocean_complete.txt")),
    ("global", "land", "TAVG",
     _S3_GLOBAL.format(fname="Complete_TAVG_complete.txt")),
    ("global", "land", "TMAX",
     _S3_GLOBAL.format(fname="Complete_TMAX_complete.txt")),
    ("global", "land", "TMIN",
     _S3_GLOBAL.format(fname="Complete_TMIN_complete.txt")),
]

# High-resolution beta product (land-only) — continent + hemisphere monthly
# series. Simple "Year, Month, Anomaly, Unc." layout.
_CONTINENTS = [
    "africa", "antarctica", "asia", "australia",
    "europe", "north-america", "oceania", "south-america",
]
_HEMISPHERES = ["northern-hemisphere", "southern-hemisphere"]
_REGION_VARS = ["TAVG", "TMAX", "TMIN"]

_REGIONAL_SCHEMA = pa.schema([
    ("region", pa.string()),
    ("surface", pa.string()),
    ("variable", pa.string()),
    ("year", pa.int16()),
    ("month", pa.int8()),
    ("anomaly", pa.float64()),
    ("uncertainty", pa.float64()),
])


def _hr_regional_series():
    """Yield (region, surface, variable, url) for the HR continent/hemisphere
    monthly text series."""
    for var in _REGION_VARS:
        for region in _CONTINENTS:
            yield (region, "land", var, _HR.format(
                path=f"time-series/Regions/Continents/{var}/{region}-{var}-monthly.txt"))
        for region in _HEMISPHERES:
            yield (region, "land", var, _HR.format(
                path=f"time-series/Regions/Other/{var}/{region}-{var}-monthly.txt"))


def _parse_anomaly_series(text: str):
    """Parse a Berkeley Earth whitespace-delimited monthly anomaly file.

    Both layouts (global "complete" and HR regional) share the first four
    columns: Year, Month, Monthly Anomaly, Uncertainty. The global files carry
    additional moving-average columns we ignore, and contain a SECOND data
    section (sea-ice handled via water temperature) that restarts the calendar
    — we stop at the restart so only the first (air-temperature) section is
    kept. Yields (year, month, anomaly|None, uncertainty|None).
    """
    prev_key = -1
    for line in text.splitlines():
        s = line.strip()
        if not s or s[0] == "%":
            continue
        p = s.split()
        if len(p) < 4:
            continue
        try:
            year = int(p[0])
            month = int(p[1])
            anom = float(p[2])
            unc = float(p[3])
        except ValueError:
            continue
        if not (1 <= month <= 12):
            continue
        key = year * 12 + month
        if key <= prev_key:
            # Calendar restarted → second section; stop.
            break
        prev_key = key
        yield (
            year,
            month,
            None if math.isnan(anom) else anom,
            None if math.isnan(unc) else unc,
        )


def fetch_regional(node_id: str) -> None:
    """Download every global/continent/hemisphere monthly series and publish
    one long-format parquet."""
    asset = node_id
    regions, surfaces, variables, years, months, anomalies, uncertainties = (
        [], [], [], [], [], [], [])

    series = list(_GLOBAL_SERIES) + list(_hr_regional_series())
    for region, surface, variable, url in series:
        text = _http(url).text
        n = 0
        for year, month, anom, unc in _parse_anomaly_series(text):
            regions.append(region)
            surfaces.append(surface)
            variables.append(variable)
            years.append(year)
            months.append(month)
            anomalies.append(anom)
            uncertainties.append(unc)
            n += 1
        if n == 0:
            raise AssertionError(f"no data rows parsed from {url}")
        print(f"  {region}/{surface}/{variable}: {n} rows", flush=True)

    table = pa.table({
        "region": pa.array(regions, pa.string()),
        "surface": pa.array(surfaces, pa.string()),
        "variable": pa.array(variables, pa.string()),
        "year": pa.array(years, pa.int16()),
        "month": pa.array(months, pa.int8()),
        "anomaly": pa.array(anomalies, pa.float64()),
        "uncertainty": pa.array(uncertainties, pa.float64()),
    }, schema=_REGIONAL_SCHEMA)
    save_raw_parquet(table, asset)


# ===========================================================================
# Entity 2: station-observations
# ===========================================================================

_QC_ZIP = ("https://storage.googleapis.com/berkeley-earth-stations/"
           "Berkeley-Earth/{var}/Monthly/LATEST%20-%20Quality%20Controlled.zip")
_STATION_VARS = ["TAVG", "TMAX", "TMIN"]
_BATCH_ROWS = 1_000_000

_STATION_SCHEMA = pa.schema([
    ("station_id", pa.int32()),
    ("variable", pa.string()),
    ("year", pa.int16()),
    ("month", pa.int8()),
    ("temperature_c", pa.float64()),
    ("uncertainty_c", pa.float64()),
])


def _zip_total_size(url: str) -> int:
    r = _http(url, headers={"Range": "bytes=0-0"})
    cr = r.headers.get("content-range")
    if cr and "/" in cr:
        return int(cr.rsplit("/", 1)[-1])
    return int(r.headers["content-length"])


def _zip_member_location(url: str, member: str) -> tuple[int, int, int]:
    """Return (data_start, compressed_size, method) for a zip member via
    range requests against the End-Of-Central-Directory record."""
    total = _zip_total_size(url)

    tail_len = min(65536, total)
    tail = _http(url, headers={"Range": f"bytes={total - tail_len}-{total - 1}"}).content
    eocd = tail.rfind(b"PK\x05\x06")
    if eocd < 0:
        raise AssertionError(f"no EOCD record found in {url}")
    cd_size, cd_offset = struct.unpack("<II", tail[eocd + 12:eocd + 20])

    cd = _http(url, headers={"Range": f"bytes={cd_offset}-{cd_offset + cd_size - 1}"}).content
    i = 0
    target = member.encode()
    while i < len(cd) and cd[i:i + 4] == b"PK\x01\x02":
        method = struct.unpack("<H", cd[i + 10:i + 12])[0]
        comp_size = struct.unpack("<I", cd[i + 20:i + 24])[0]
        fn_len, ex_len, cm_len = struct.unpack("<HHH", cd[i + 28:i + 34])
        lho = struct.unpack("<I", cd[i + 42:i + 46])[0]
        name = cd[i + 46:i + 46 + fn_len]
        if name == target:
            lh = _http(url, headers={"Range": f"bytes={lho}-{lho + 29}"}).content
            lfn, lex = struct.unpack("<HH", lh[26:30])
            return lho + 30 + lfn + lex, comp_size, method
        i += 46 + fn_len + ex_len + cm_len
    raise AssertionError(f"member {member!r} not found in {url}")


def _inflate_lines(compressed: bytes, method: int):
    """Yield decoded text lines from a (possibly deflated) zip member without
    materialising the full decompressed payload."""
    if method == 0:  # stored
        for ln in compressed.split(b"\n"):
            yield ln.decode("ascii", "replace")
        return
    if method != 8:
        raise AssertionError(f"unsupported zip compression method {method}")

    dec = zlib.decompressobj(-15)
    leftover = b""
    chunk = 8 << 20
    for i in range(0, len(compressed), chunk):
        out = dec.decompress(compressed[i:i + chunk])
        if not out:
            continue
        data = leftover + out
        parts = data.split(b"\n")
        leftover = parts.pop()
        for ln in parts:
            yield ln.decode("ascii", "replace")
    tail = leftover + dec.flush()
    for ln in tail.split(b"\n"):
        if ln:
            yield ln.decode("ascii", "replace")


def _make_station_batch(var, sids, years, months, temps, uncs):
    n = len(sids)
    return pa.record_batch([
        pa.array(sids, pa.int32()),
        pa.array([var] * n, pa.string()),
        pa.array(years, pa.int16()),
        pa.array(months, pa.int8()),
        pa.array(temps, pa.float64()),
        pa.array(uncs, pa.float64()),
    ], schema=_STATION_SCHEMA)


def fetch_stations(node_id: str) -> None:
    """Stream the Quality-Controlled `data.txt` for each variable and publish
    one batched parquet of per-station monthly observations."""
    asset = node_id
    grand_total = 0
    with raw_parquet_writer(asset, _STATION_SCHEMA) as writer:
        for var in _STATION_VARS:
            url = _QC_ZIP.format(var=var)
            data_start, comp_size, method = _zip_member_location(url, "data.txt")
            member = _http(
                url,
                headers={"Range": f"bytes={data_start}-{data_start + comp_size - 1}"},
            ).content

            sids, years, months, temps, uncs = [], [], [], [], []
            var_rows = 0
            for line in _inflate_lines(member, method):
                if not line or line[0] == "%":
                    continue
                p = line.split("\t")
                if len(p) < 5:
                    continue
                try:
                    sid = int(p[0])
                    date = float(p[2])
                    temp = float(p[3])
                    unc = float(p[4])
                except ValueError:
                    continue
                year = int(date)
                month = int(round((date - year) * 12 + 0.5))
                if month < 1:
                    month = 1
                elif month > 12:
                    month = 12
                sids.append(sid)
                years.append(year)
                months.append(month)
                temps.append(temp)
                uncs.append(unc)
                if len(sids) >= _BATCH_ROWS:
                    writer.write_batch(
                        _make_station_batch(var, sids, years, months, temps, uncs))
                    var_rows += len(sids)
                    sids, years, months, temps, uncs = [], [], [], [], []
            if sids:
                writer.write_batch(
                    _make_station_batch(var, sids, years, months, temps, uncs))
                var_rows += len(sids)
            if var_rows == 0:
                raise AssertionError(f"no station rows parsed for {var} from {url}")
            grand_total += var_rows
            print(f"  {var}: {var_rows:,} rows (running total {grand_total:,})", flush=True)


# ===========================================================================
# Specs
# ===========================================================================

DOWNLOAD_SPECS = [
    NodeSpec(id="berkeley-earth-regional-temperature-series",
             fn=fetch_regional, kind="download"),
    NodeSpec(id="berkeley-earth-station-observations",
             fn=fetch_stations, kind="download"),
]

TRANSFORM_SPECS = [
    SqlNodeSpec(
        id="berkeley-earth-regional-temperature-series-transform",
        deps=["berkeley-earth-regional-temperature-series"],
        sql='''
            SELECT
                region,
                surface,
                variable,
                make_date(CAST(year AS INTEGER), CAST(month AS INTEGER), 1) AS date,
                CAST(year AS INTEGER)  AS year,
                CAST(month AS INTEGER) AS month,
                anomaly,
                uncertainty
            FROM "berkeley-earth-regional-temperature-series"
            WHERE anomaly IS NOT NULL
        ''',
    ),
    SqlNodeSpec(
        id="berkeley-earth-station-observations-transform",
        deps=["berkeley-earth-station-observations"],
        sql='''
            SELECT
                station_id,
                variable,
                make_date(CAST(year AS INTEGER), CAST(month AS INTEGER), 1) AS date,
                CAST(year AS INTEGER)  AS year,
                CAST(month AS INTEGER) AS month,
                temperature_c,
                uncertainty_c
            FROM "berkeley-earth-station-observations"
            WHERE temperature_c IS NOT NULL
            QUALIFY row_number() OVER (
                PARTITION BY station_id, variable, year, month
                ORDER BY uncertainty_c
            ) = 1
        ''',
    ),
]
