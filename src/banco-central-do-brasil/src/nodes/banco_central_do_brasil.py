"""Banco Central do Brasil — download step.

Discovery and fetch are routed through the CKAN catalog at
dadosabertos.bcb.gov.br (the chosen mechanism). Every entity in the union is
a CKAN package, except two aggregate entities:

  * sgs-series — the catalog of the ~3.4k SGS time series (CKAN packages that
                 carry a ``codigo_sgs``), collapsed into one directory file.
  * sgs-values — the (series_code, date, value) observations for every SGS
                 series, fetched per-series from the SGS REST API.

For an ordinary CKAN package, ``package_show`` yields ``resources[]``; each
resource is routed by format:
  * CSV / ZIP / XLS(X) / TXT — direct file download.
  * OData — the Olinda OData service root; entity sets are enumerated from
            the service document and paginated with $top/$skip.
  * HTML / API / PDF / navigator-JSON links — skipped (not tabular data).

Idempotency is per-resource: ``package_show`` (cheap) always runs, then each
resource / OData entity set / SGS series is short-circuited individually via
``raw_asset_exists``.
"""

import json
import re
import time
from datetime import datetime, timezone

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
    save_raw_json,
    raw_writer,
    raw_asset_exists,
    load_state,
    save_state,
)

# --------------------------------------------------------------------------
# Entity union (44 entities). The two SGS aggregates are fetched specially;
# every other id is a CKAN package name.
# --------------------------------------------------------------------------
SGS_SERIES_ID = "sgs-series"
SGS_VALUES_ID = "sgs-values"

PACKAGE_IDS = [
    "agencias",
    "correspondentes",
    "cronograma-de-vencimentos-de-instrumentos-cambiais",
    "cronograma-de-vencimentos-de-titulos",
    "dados-cadastrais-de-entidades-autorizadas",
    "desenrola-brasil",
    "dinheiro-em-circulao",
    "dolar-americano-usd-todos-os-boletins-diarios",
    "emissoes-e-recolhimentos",
    "estatisticas-do-spi-sistema-de-pagamentos-instantaneos",
    "estatisticas-do-str-sistema-de-transferencia-de-reservas",
    "estatisticas-meios-pagamentos",
    "estatisticas-selic-operacoes",
    "estatisticas-selic-participantes",
    "estatsticas-selic---clientes",
    "estatsticas-selic---contas",
    "expectativas-mercado",
    "historico-de-atuacoes-no-mercado-de-cambio",
    "ifdata---dados-selecionados-de-instituies-financeiras",
    "ifs-balancetes",
    "informacoes-do-mercado-imobiliario",
    "leiloes-selic",
    "matrizdadoscreditorural",
    "negociacao-de-titulos-federais-no-mercado-secundario",
    "orcamento-de-autoridade-monetaria-2024",
    "pix",
    "precos-de-titulos-publicos-para-redesconto",
    "publicao-dos-registros-de-capitais-estrangeiros",
    "quantidades-produzidas-por-ano-e-especie-cedulas-e-moedas",
    "ranking-de-instituicoes-por-indice-de-reclamacoes",
    "ranking-do-vet",
    "recolhimentos-compulsorios-quadro-resumo",
    "relacao-de-instituicoes-em-funcionamento-no-pais",
    "saldos-contabeis-mensais-bcb",
    "scr-por-sub-regiao",
    "scr_data",
    "sistema-de-registro-de-operacoes-de-credito-com-o-setor-publico-cadip",
    "tarifa-bancaria-valores-minimos-maximos-e-medios",
    "tarifas-bancarias-por-segmento-e-por-instituicao",
    "tarifas-bancarias-por-segmento-e-por-servicos-em-ordem-decrescente-de-valores",
    "taxas-de-cambio-todos-os-boletins-diarios",
    "taxas-de-juros-de-operacoes-de-credito",
]

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------
SLUG = "banco-central-do-brasil"
CKAN_BASE = "https://dadosabertos.bcb.gov.br/api/3/action"
SGS_BASE = "https://api.bcb.gov.br/dados/serie"

_CONNECT_READ = (15.0, 300.0)        # (connect, read) seconds — large OData / file payloads
_ODATA_PAGE = 1000                   # $top — Olinda accepts up to 1000 rows/page
_MAX_ODATA_PAGES = 20000             # safety cap: 20M rows per entity set
_SGS_WINDOW_YEARS = 10               # SGS daily series cap window (introduced 2025-03-26)
_SGS_FIRST_YEAR = 1980               # earliest plausible SGS start; empty early windows are harmless
_SKIP_TTL_SECONDS = 14 * 86400       # how long a permanent-error skip marker holds

# Resource formats that are direct file downloads.
_DIRECT_FORMATS = {"CSV", "ZIP", "CSV.ZIP", "XLS", "XLSX", "XLSM", "TXT", "TSV"}
_FILE_SUFFIXES = (".csv", ".zip", ".txt", ".xls", ".xlsx", ".xlsm", ".tsv", ".json")


# --------------------------------------------------------------------------
# Transport — retry on transient failures only
# --------------------------------------------------------------------------
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
    stop=stop_after_attempt(8),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _request(url: str, params: dict | None = None) -> httpx.Response:
    """GET with backoff. 4xx (except 429) surface immediately for the caller
    to classify as permanent; 5xx/429/timeouts are retried."""
    resp = get(url, params=params, timeout=_CONNECT_READ)
    resp.raise_for_status()
    return resp


def _get_json(url: str, params: dict | None = None):
    return _request(url, params).json()


# --------------------------------------------------------------------------
# CKAN helpers
# --------------------------------------------------------------------------
def _ckan_package(name: str) -> dict:
    """Full package metadata (incl. resources[]) for one CKAN package."""
    result = _get_json(f"{CKAN_BASE}/package_show", {"id": name})
    return result["result"]


def _ckan_scan() -> list:
    """Every package in the catalog, with full metadata. ~4.1k packages,
    paginated 1000 at a time."""
    packages: list = []
    start = 0
    while True:
        result = _get_json(
            f"{CKAN_BASE}/package_search", {"rows": 1000, "start": start}
        )["result"]
        batch = result.get("results", [])
        if not batch:
            break
        packages.extend(batch)
        total = result.get("count", 0)
        start += len(batch)
        print(f"[{SLUG}] CKAN scan: {start}/{total} packages", flush=True)
        if total and start >= total:
            break
        if start > 20000:  # safety cap — catalog is ~4.1k
            raise RuntimeError(f"CKAN scan exceeded 20000 packages at start={start}")
    return packages


# --------------------------------------------------------------------------
# Generic helpers
# --------------------------------------------------------------------------
def _slug(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return s or "x"


def _spec_id(entity_id: str) -> str:
    return f"{SLUG}-{entity_id.lower().replace('_', '-')}"


def _file_ext(url: str, fmt: str) -> str:
    """Best-effort file extension for a direct download."""
    path = url.split("#")[0].split("?")[0].rstrip("/")
    base = path.rsplit("/", 1)[-1]
    if "." in base:
        ext = base.split(".", 1)[1].lower()
        ext = re.sub(r"[^a-z0-9.]+", "", ext)
        if ext:
            return ext
    fmt = re.sub(r"[^a-z0-9.]+", "", (fmt or "").lower())
    return fmt or "dat"


def _expire_skips(state: dict) -> dict:
    """Drop skip markers whose TTL has elapsed so source recovery is automatic."""
    now = int(time.time())
    skipped = {
        k: v for k, v in state.get("skipped", {}).items()
        if isinstance(v, dict) and v.get("expires_at", 0) > now
    }
    state["skipped"] = skipped
    return state


# --------------------------------------------------------------------------
# OData (Olinda) fetch
# --------------------------------------------------------------------------
def _odata_entity_sets(odata_root: str) -> list:
    """Entity-set names from an Olinda OData service document. Skips the
    underscore-prefixed internal duplicates Olinda exposes."""
    doc = _get_json(odata_root)
    names = []
    for entry in doc.get("value", []):
        name = entry.get("name")
        if name and not name.startswith("_"):
            names.append(name)
    return names


def _odata_fetch(asset_id: str, odata_root: str, eset: str) -> int:
    """Paginate one OData entity set into a gzip ndjson raw file.

    Returns the row count. Returns -1 if the entity set itself is broken —
    that is skipped, not fatal. The service document already loaded for this
    service (see fetch_package), so the service is confirmed healthy: any
    HTTP error answered on the first page is specific to THIS entity set.
    A 4xx is a parametrized / unavailable set; a 5xx that survived the retry
    decorator is a structurally broken set (e.g. Olinda answers 500 on the
    'Atual' projection of PixLiquidados). Either way it is one bad entity set
    on a healthy service, so we skip it rather than fail the whole package.
    Mid-crawl errors are NOT swallowed — they propagate, since a failure
    after page 0 most likely signals the service going down under us."""
    url = odata_root + eset
    # Probe the first page; both parametrized (4xx) and structurally broken
    # (persistent 5xx) entity sets surface here.
    try:
        first = _get_json(url, {"$top": _ODATA_PAGE, "$skip": 0, "$format": "json"})
    except httpx.HTTPStatusError as exc:
        print(f"[{SLUG}] OData {eset}: skipped (HTTP {exc.response.status_code} "
              f"on first page — entity set broken or parametrized)", flush=True)
        return -1

    rows = first.get("value", [])
    total = 0
    with raw_writer(asset_id, "ndjson.gz", mode="wt", compression="gzip") as fh:
        page = 0
        while True:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            total += len(rows)
            page += 1
            if len(rows) < _ODATA_PAGE:
                break
            if page >= _MAX_ODATA_PAGES:
                raise RuntimeError(
                    f"OData {eset} exceeded {_MAX_ODATA_PAGES} pages — "
                    f"source grew past the safety cap")
            if page % 50 == 0:
                print(f"[{SLUG}] OData {eset}: {total} rows", flush=True)
            data = _get_json(
                url, {"$top": _ODATA_PAGE, "$skip": page * _ODATA_PAGE,
                      "$format": "json"})
            rows = data.get("value", [])
            if not rows:
                break
    return total


# --------------------------------------------------------------------------
# fetch fn — ordinary CKAN package
# --------------------------------------------------------------------------
def fetch_package(entity_id: str) -> None:
    """Fetch every tabular resource of one CKAN package: direct files plus
    every entity set of an Olinda OData service, if present."""
    spec_id = _spec_id(entity_id)
    state = _expire_skips(load_state(spec_id))

    pkg = _ckan_package(entity_id)
    resources = pkg.get("resources", []) or []

    covered = 0          # resources that produced data or were already fresh
    rows_total = 0
    seen_formats: list = []

    for res in resources:
        fmt = (res.get("format") or "").strip().upper()
        url = (res.get("url") or "").strip()
        seen_formats.append(fmt or "?")
        if not url:
            continue

        # --- Olinda OData service root -----------------------------------
        if fmt == "ODATA":
            root = url if url.endswith("/") else url + "/"
            try:
                esets = _odata_entity_sets(root)
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                if 400 <= code < 500:
                    # The service document itself is gone/forbidden — the
                    # whole Olinda service is unusable; skip it as a resource.
                    print(f"[{SLUG}] {entity_id}: OData service doc {code} at "
                          f"{root} — skipped", flush=True)
                    continue
                # 5xx on the service document: the Olinda service is down,
                # not one bad entity set. Fail honestly so the harness retries
                # the step once Olinda recovers — do NOT silently skip.
                raise
            for eset in esets:
                od_asset = f"{spec_id}--od-{_slug(eset)}"
                if raw_asset_exists(od_asset, "ndjson.gz", max_age_days=1):
                    # BCB datasets refresh at most daily (many are daily feeds)
                    covered += 1
                    continue
                n = _odata_fetch(od_asset, root, eset)
                if n >= 0:
                    covered += 1
                    rows_total += n
            continue

        # --- direct file download ----------------------------------------
        if "#!" in url:
            continue  # Olinda data-navigator UI link, not a file
        is_file = fmt in _DIRECT_FORMATS or url.split("?")[0].lower().endswith(_FILE_SUFFIXES)
        if not is_file:
            continue  # HTML / API / PDF / documentation

        ext = _file_ext(url, fmt)
        res_key = (res.get("id") or _slug(res.get("name") or url))[:16]
        res_asset = f"{spec_id}--r-{res_key}"
        if raw_asset_exists(res_asset, ext, max_age_days=1):
            # BCB open-data files refresh at most daily per the CKAN portal
            covered += 1
            continue
        try:
            resp = _request(url)
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            if 400 <= code < 500:
                print(f"[{SLUG}] {entity_id}: resource {url} -> {code}, skipped",
                      flush=True)
                continue
            raise
        save_raw_file(resp.content, res_asset, extension=ext)
        covered += 1
        rows_total += len(resp.content)

    if covered == 0:
        raise RuntimeError(
            f"{entity_id}: no downloadable data resources "
            f"(formats seen: {sorted(set(seen_formats))})")

    state["schema_version"] = 1
    state["last_run_stats"] = {
        "resources": len(resources),
        "covered": covered,
        "bytes_or_rows": rows_total,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    save_state(spec_id, state)
    print(f"[{SLUG}] {entity_id}: covered {covered} resources", flush=True)


# --------------------------------------------------------------------------
# fetch fn — SGS series catalog
# --------------------------------------------------------------------------
_CATALOG_FIELDS = (
    "name", "title", "codigo_sgs", "tipo_serie", "frequencia", "periodicidade",
    "unidade_medida", "inicio_periodo", "fim_periodo", "metadata_modified",
    "metadata_created", "referencias", "notes", "author", "maintainer",
    "license_id", "version",
)


def fetch_sgs_series(entity_id: str) -> None:
    """Build the SGS time-series directory: one row per CKAN package that
    carries a ``codigo_sgs``."""
    spec_id = _spec_id(entity_id)
    if raw_asset_exists(spec_id, "json", max_age_days=7):
        # SGS series membership changes slowly; a weekly re-scan is ample
        print(f"[{SLUG}] sgs-series catalog still fresh — skipping scan", flush=True)
        return

    catalog = []
    for pkg in _ckan_scan():
        if not pkg.get("codigo_sgs"):
            continue
        row = {k: pkg[k] for k in _CATALOG_FIELDS if pkg.get(k) not in (None, "", [])}
        org = pkg.get("organization")
        if isinstance(org, dict):
            row["organization"] = org.get("name") or org.get("title")
        catalog.append(row)

    if not catalog:
        raise RuntimeError("sgs-series: CKAN scan found no codigo_sgs packages")

    save_raw_json(catalog, spec_id)
    state = load_state(spec_id)
    state["schema_version"] = 1
    state["last_run_stats"] = {
        "series": len(catalog),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    save_state(spec_id, state)
    print(f"[{SLUG}] sgs-series: catalogued {len(catalog)} series", flush=True)


# --------------------------------------------------------------------------
# fetch fn — SGS series observations
# --------------------------------------------------------------------------
def _sgs_call(code: str, di: str | None = None, df: str | None = None) -> list:
    """One SGS REST call. Raises httpx.HTTPStatusError for 4xx (caller
    classifies); transient failures are retried inside ``_request``."""
    params = {"formato": "json"}
    if di:
        params["dataInicial"] = di
    if df:
        params["dataFinal"] = df
    resp = _request(f"{SGS_BASE}/bcdata.sgs.{code}/dados", params)
    text = resp.text.strip()
    if not text:
        return []
    data = resp.json()
    return data if isinstance(data, list) else []


def _sgs_windowed(code: str) -> list:
    """Fetch a long daily series in <=10-year windows (the SGS daily-series
    cap). Per-window 4xx are skipped; transient failures still propagate."""
    rows: list = []
    seen: set = set()
    end_year = datetime.now(timezone.utc).year
    year = _SGS_FIRST_YEAR
    while year <= end_year:
        last = min(year + _SGS_WINDOW_YEARS - 1, end_year)
        try:
            window = _sgs_call(code, f"01/01/{year}", f"31/12/{last}")
        except httpx.HTTPStatusError as exc:
            if 400 <= exc.response.status_code < 500:
                window = []
            else:
                raise
        for r in window:
            d = r.get("data")
            if d not in seen:
                seen.add(d)
                rows.append(r)
        year = last + 1
    return rows


def _is_daily(freq: str, tipo: str) -> bool:
    blob = f"{freq} {tipo}".lower()
    return "diár" in blob or "diar" in blob


def fetch_sgs_values(entity_id: str) -> None:
    """Fetch (date, value) observations for every SGS series, one raw file
    per series. The full series is re-fetched once it goes stale — SGS
    payloads are small and this captures revisions cleanly."""
    spec_id = _spec_id(entity_id)
    state = _expire_skips(load_state(spec_id))
    if state.get("schema_version") not in (None, 1):
        print(f"[{SLUG}] sgs-values: unknown schema_version "
              f"{state.get('schema_version')} — resetting state", flush=True)
        state = {"skipped": {}}

    # Discover series codes + frequency from the CKAN catalog.
    series: list = []
    for pkg in _ckan_scan():
        code = pkg.get("codigo_sgs")
        if not code:
            continue
        series.append((
            str(code).strip(),
            pkg.get("frequencia") or pkg.get("periodicidade") or "",
            pkg.get("tipo_serie") or "",
        ))
    if not series:
        raise RuntimeError("sgs-values: CKAN scan found no codigo_sgs packages")

    fetched = empty = skipped = fresh = 0
    for idx, (code, freq, tipo) in enumerate(series, 1):
        if idx % 250 == 0:
            print(f"[{SLUG}] sgs-values: {idx}/{len(series)} series "
                  f"(fetched={fetched} fresh={fresh} skipped={skipped})", flush=True)
            save_state(spec_id, {**state, "schema_version": 1})

        asset = f"{spec_id}--{code}"
        if _is_daily(freq, tipo):
            is_fresh = raw_asset_exists(asset, "json", max_age_days=1)
            # daily SGS series publish every business day
        else:
            is_fresh = raw_asset_exists(asset, "json", max_age_days=7)
            # monthly/quarterly/annual SGS series — weekly re-check is ample
        if is_fresh:
            fresh += 1
            continue

        try:
            data = _sgs_call(code)
        except httpx.HTTPStatusError as exc:
            sc = exc.response.status_code
            if sc == 406:                       # window too large — chunk it
                data = _sgs_windowed(code)
            elif 400 <= sc < 500:               # permanent — TTL skip marker
                state.setdefault("skipped", {})[code] = {
                    "reason": f"http {sc}",
                    "expires_at": int(time.time()) + _SKIP_TTL_SECONDS,
                }
                skipped += 1
                continue
            else:
                raise

        if not data:
            empty += 1
            continue
        save_raw_json(data, asset)
        fetched += 1

    state["schema_version"] = 1
    state["last_run_stats"] = {
        "series_total": len(series),
        "fetched": fetched,
        "fresh": fresh,
        "empty": empty,
        "skipped": skipped,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    save_state(spec_id, state)
    print(f"[{SLUG}] sgs-values: {len(series)} series — fetched={fetched} "
          f"fresh={fresh} empty={empty} skipped={skipped}", flush=True)


# --------------------------------------------------------------------------
# DOWNLOAD_SPECS — one per entity-union member
# --------------------------------------------------------------------------
DOWNLOAD_SPECS = [
    NodeSpec(
        id=_spec_id(eid),
        fn=fetch_package,
        args=(eid,),
        deps=(),
        kind="download",
    )
    for eid in PACKAGE_IDS
] + [
    NodeSpec(
        id=_spec_id(SGS_SERIES_ID),
        fn=fetch_sgs_series,
        args=(SGS_SERIES_ID,),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id=_spec_id(SGS_VALUES_ID),
        fn=fetch_sgs_values,
        args=(SGS_VALUES_ID,),
        deps=(),
        kind="download",
    ),
]
