"""Banco de Mexico — SIE (Sistema de Informacion Economica) download node.

Two collect entities, one NodeSpec each:

  - series : per-series metadata catalog (idSerie, titulo, periodicidad, ...)
  - values : long-format time-series observations (idSerie, fecha, dato)

The SIE REST API has NO list-all/catalog endpoint, so series IDs are first
discovered by scraping the SieInternet HTML directory:

    sector directory page  (accion=consultarDirectorioCuadros)
        -> information-structure table pages (accion=consultarCuadro)
            -> per-series links carry the idSerie in their `s=` query param

Those table pages are server-rendered HTML — the series IDs sit in plain
anchor hrefs. The "summary"/analytic tables (accion=consultarCuadroAnalitico)
render their series client-side via JavaScript and are therefore NOT scrapable
here; their aggregate series are not covered by this connector. The discovered
ID set is cached in shared state so the two specs don't each re-walk ~800
table pages.

Auth: the SIE REST API requires a free token (one-time, CAPTCHA-walled
generation at https://www.banxico.org.mx/SieAPIRest/service/v1/token). It must
be pre-provisioned as the BANXICO_TOKEN env var and is sent as the Bmx-Token
header on every REST call. Without it the REST fetch fns fail loudly.

Whole-source pattern: full history is re-fetched per series each run (the API
serves a single response per series with no pagination). Idempotency is gated
on raw-file freshness plus a successful prior run recorded in state.
"""

import os
import re
from datetime import datetime, timedelta, timezone

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
    get,
    load_state,
    raw_asset_exists,
    raw_parquet_writer,
    save_raw_parquet,
    save_state,
)

# --- endpoints --------------------------------------------------------------
SIE_BASE = "https://www.banxico.org.mx/SieAPIRest/service/v1"
DIRECTORY_URL = (
    "https://www.banxico.org.mx/SieInternet/consultarDirectorioInternetAction.do"
)

# SIE currently exposes ~26 sectors; probe 1..30 and skip empties so newly
# added sectors are picked up automatically without a code change.
SECTOR_RANGE = range(1, 31)

# REST accepts up to 20 series IDs per request (documented mechanism cap).
BATCH_SIZE = 20

# Shared catalog cache — both specs reuse one directory walk.
CATALOG_STATE = "banco-de-m-xico-catalog"
CATALOG_MAX_AGE_DAYS = 30  # SIE series catalog changes slowly (new series rare)

# Table links in the sector directory pages, e.g.
#   accion=consultarCuadro&idCuadro=CE174
# The `&`-anchored regex naturally excludes accion=consultarCuadroAnalitico.
_CUADRO_RE = re.compile(r"accion=consultarCuadro(?:&amp;|&)idCuadro=([A-Z]{2}\d+)")

# Per-series links on a table page, e.g.  ?s=SE44352,CE174,1&l=en
_SERIES_RE = re.compile(r"[?&]s=([A-Z]+\d+),")

# Metadata columns kept from /series/{ids} responses (all stored as strings).
_SERIES_COLUMNS = [
    "idSerie",
    "titulo",
    "fechaInicio",
    "fechaFin",
    "periodicidad",
    "cifra",
    "unidad",
]
_SERIES_SCHEMA = pa.schema([(c, pa.string()) for c in _SERIES_COLUMNS])
_VALUES_SCHEMA = pa.schema(
    [("idSerie", pa.string()), ("fecha", pa.string()), ("dato", pa.string())]
)

# (connect, read, write, pool) — read is generous: a 20-series full-history
# batch can be a large JSON response.
_TIMEOUT = httpx.Timeout(connect=10.0, read=180.0, write=30.0, pool=30.0)

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
    """True for retryable network/server failures; False for permanent 4xx."""
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


# --- HTTP primitives --------------------------------------------------------
# SieInternet documents no rate limit; 30/min is a polite ceiling for the
# one-time directory walk.
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
@sleep_and_retry
@limits(calls=30, period=60)
def _scrape(url: str, params: dict) -> str:
    resp = get(url, params=params, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.text


# SIE: 40,000 timely/metadata queries/day, burst cap 80/min — 64/min ~= 80%.
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
@sleep_and_retry
@limits(calls=64, period=60)
def _api_metadata(url: str, headers: dict) -> dict:
    resp = get(url, headers=headers, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# SIE: 10,000 historical queries/day, burst cap 200 per 5 min — 160/5min ~= 80%.
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
@sleep_and_retry
@limits(calls=160, period=300)
def _api_values(url: str, headers: dict) -> dict:
    resp = get(url, headers=headers, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# --- helpers ----------------------------------------------------------------
def _batches(items, size):
    """Yield successive `size`-length chunks of `items`."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _clean(value):
    """Normalize an API scalar to a string (or None) for parquet storage."""
    return None if value is None else str(value)


def _require_token() -> str:
    """Return the SIE token from the environment, or fail loudly."""
    token = os.environ.get("BANXICO_TOKEN")
    if not token:
        raise RuntimeError(
            "BANXICO_TOKEN is not set. The Banxico SIE REST API requires a free "
            "token (one-time CAPTCHA-walled generation at "
            "https://www.banxico.org.mx/SieAPIRest/service/v1/token). Provision "
            "it in the connector environment before running this connector."
        )
    return token


def _discover_series_ids() -> list:
    """Return every SIE series ID, scraping the SieInternet HTML directory.

    The result is cached in shared state for CATALOG_MAX_AGE_DAYS so the two
    download specs reuse a single directory walk instead of each scraping
    ~800 table pages.
    """
    state = load_state(CATALOG_STATE)
    if state.get("schema_version") == 1:
        ids = state.get("series_ids")
        discovered_at = state.get("discovered_at")
        if ids and discovered_at:
            age = datetime.now(timezone.utc) - datetime.fromisoformat(discovered_at)
            if age < timedelta(days=CATALOG_MAX_AGE_DAYS):
                print(f"  catalog: reusing {len(ids)} cached series ids", flush=True)
                return sorted(ids)
    elif state:
        print(
            f"  catalog: unknown state schema {state.get('schema_version')!r} "
            "- re-discovering",
            flush=True,
        )

    # 1) sector pages -> table (cuadro) IDs
    cuadros = set()
    for sector in SECTOR_RANGE:
        html = _scrape(
            DIRECTORY_URL,
            {"accion": "consultarDirectorioCuadros", "sector": sector, "locale": "en"},
        )
        found = set(_CUADRO_RE.findall(html))
        cuadros |= found
        if found:
            print(f"  catalog: sector {sector} -> {len(found)} tables", flush=True)
    if not cuadros:
        raise RuntimeError(
            "SieInternet directory scrape found 0 tables - the page structure "
            "likely changed; the discovery regex needs updating."
        )

    # 2) table pages -> series IDs
    cuadros = sorted(cuadros)
    series_ids = set()
    for i, cuadro in enumerate(cuadros, 1):
        html = _scrape(
            DIRECTORY_URL,
            {"accion": "consultarCuadro", "idCuadro": cuadro, "locale": "en"},
        )
        series_ids |= set(_SERIES_RE.findall(html))
        if i % 50 == 0:
            print(
                f"  catalog: {i}/{len(cuadros)} tables walked, "
                f"{len(series_ids)} unique series so far",
                flush=True,
            )
    if not series_ids:
        raise RuntimeError(
            "SieInternet table scrape found 0 series across "
            f"{len(cuadros)} tables - the page structure likely changed."
        )

    series_ids = sorted(series_ids)
    save_state(
        CATALOG_STATE,
        {
            "schema_version": 1,
            "series_ids": series_ids,
            "discovered_at": datetime.now(timezone.utc).isoformat(),
            "tables_walked": len(cuadros),
        },
    )
    print(
        f"  catalog: discovered {len(series_ids)} series across "
        f"{len(cuadros)} tables",
        flush=True,
    )
    return series_ids


# --- fetch fns --------------------------------------------------------------
def fetch_series(entity_id: str) -> None:
    """Download per-series metadata for the whole SIE catalog."""
    asset = f"banco-de-m-xico-{entity_id}"
    # SIE series catalog metadata changes slowly; a weekly refresh is ample.
    if (
        raw_asset_exists(asset, max_age_days=7)
        and load_state(asset).get("last_run_stats", {}).get("series", 0) > 0
    ):
        print(f"  {asset}: fresh raw + prior success - skipping", flush=True)
        return

    token = _require_token()
    headers = {"Bmx-Token": token}
    ids = _discover_series_ids()

    cols = {c: [] for c in _SERIES_COLUMNS}
    n_fetched = 0
    failed_batches = 0
    total_batches = (len(ids) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_no, batch in enumerate(_batches(ids, BATCH_SIZE), 1):
        url = f"{SIE_BASE}/series/{','.join(batch)}"
        try:
            payload = _api_metadata(url, headers)
        except httpx.HTTPStatusError as exc:
            failed_batches += 1
            print(
                f"  series: batch {batch_no} permanent failure "
                f"{exc.response.status_code} {type(exc).__name__} {url}",
                flush=True,
            )
            continue
        for s in payload.get("bmx", {}).get("series", []):
            for c in _SERIES_COLUMNS:
                cols[c].append(_clean(s.get(c)))
            n_fetched += 1
        if batch_no % 25 == 0:
            print(
                f"  series: {batch_no}/{total_batches} batches, "
                f"{n_fetched} series",
                flush=True,
            )

    if n_fetched == 0:
        raise RuntimeError(
            "series: API returned 0 metadata records "
            f"({failed_batches}/{total_batches} batches failed)"
        )

    table = pa.table(cols, schema=_SERIES_SCHEMA)
    save_raw_parquet(table, asset)  # raw before state, always
    save_state(
        asset,
        {
            "schema_version": 1,
            "last_run_stats": {
                "series": n_fetched,
                "ids_discovered": len(ids),
                "failed_batches": failed_batches,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            },
        },
    )


def fetch_values(entity_id: str) -> None:
    """Download full-history observations for every SIE series."""
    asset = f"banco-de-m-xico-{entity_id}"
    # SIE includes daily series (FX rates, securities); refresh daily.
    if (
        raw_asset_exists(asset, max_age_days=1)
        and load_state(asset).get("last_run_stats", {}).get("observations", 0) > 0
    ):
        print(f"  {asset}: fresh raw + prior success - skipping", flush=True)
        return

    token = _require_token()
    headers = {"Bmx-Token": token}
    ids = _discover_series_ids()

    total_obs = 0
    total_series = 0
    failed_batches = 0
    total_batches = (len(ids) + BATCH_SIZE - 1) // BATCH_SIZE

    with raw_parquet_writer(asset, _VALUES_SCHEMA) as writer:
        for batch_no, batch in enumerate(_batches(ids, BATCH_SIZE), 1):
            url = f"{SIE_BASE}/series/{','.join(batch)}/datos"
            try:
                payload = _api_values(url, headers)
            except httpx.HTTPStatusError as exc:
                failed_batches += 1
                print(
                    f"  values: batch {batch_no} permanent failure "
                    f"{exc.response.status_code} {type(exc).__name__} {url}",
                    flush=True,
                )
                continue

            ser, fec, dat = [], [], []
            for s in payload.get("bmx", {}).get("series", []):
                sid = s.get("idSerie")
                for d in s.get("datos") or []:
                    ser.append(sid)
                    fec.append(_clean(d.get("fecha")))
                    dat.append(_clean(d.get("dato")))
                total_series += 1
            if ser:
                writer.write_table(
                    pa.table(
                        {"idSerie": ser, "fecha": fec, "dato": dat},
                        schema=_VALUES_SCHEMA,
                    )
                )
                total_obs += len(ser)
            if batch_no % 25 == 0:
                print(
                    f"  values: {batch_no}/{total_batches} batches, "
                    f"{total_series} series, {total_obs} observations",
                    flush=True,
                )

    if total_obs == 0:
        raise RuntimeError(
            "values: API returned 0 observations "
            f"({failed_batches}/{total_batches} batches failed)"
        )

    save_state(
        asset,
        {
            "schema_version": 1,
            "last_run_stats": {
                "observations": total_obs,
                "series": total_series,
                "ids_discovered": len(ids),
                "failed_batches": failed_batches,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            },
        },
    )


DOWNLOAD_SPECS = [
    NodeSpec(
        id="banco-de-m-xico-series",
        fn=fetch_series,
        args=("series",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="banco-de-m-xico-values",
        fn=fetch_values,
        args=("values",),
        deps=(),
        kind="download",
    ),
]
