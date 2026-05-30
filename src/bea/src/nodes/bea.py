"""BEA (Bureau of Economic Analysis) download step.

One DOWNLOAD_SPEC per collect entity — the entities ARE the BEA REST datasets
(NIPA, ITA, Regional, ...). The chosen access mechanism is the BEA REST API
at https://apps.bea.gov/api/data/ (free 36-char UserID via the BEA_API_KEY
env var), with one exception flagged by research: the **Regional** dataset is
fetched from the persistent regional bulk ZIPs (apps.bea.gov/regional/zip/),
because a REST walk of Regional is a TableName x LineCode x GeoFips cartesian
of millions of calls while a handful of stable ZIPs cover the same ground.

How a dataset is crawled (REST datasets):
  GetParameterList tells us the required parameters per dataset. For each
  dataset we enumerate ONE driving parameter via GetParameterValues (the
  TableName / TableID / Indicator / ... that segments the dataset) and issue
  one GetData call per value, pulling Year=ALL (and all frequencies). The BEA
  API rejects multi-value requests on these driving params with error 301/331
  ("exactly one Indicator/Industry must be requested"), so single-value
  iteration is mandatory, not an optimisation. MNE has no single driving param,
  so it uses an explicit (direction x classification) combo list.

Incremental contract: BEA exposes NO since/modifiedAfter filter on any method,
so a refresh is a full re-crawl. To stay bounded per run, each fetch fn keeps a
state watermark = the set of batch keys already fetched this crawl, capped by a
wall-clock budget; it returns cleanly when the budget is hit and resumes from
the watermark next run. When a crawl completes, the next (maintain-gated) run
starts a fresh epoch and re-fetches — picking up BEA's scheduled revisions.

Raw format: NDJSON per batch (one GetData result set = one file). GetData rows
are long-format dicts whose columns vary across datasets (and occasionally
carry optional keys), so NDJSON is the safe, drift-tolerant choice over a rigid
parquet schema. Regional ZIPs are saved as opaque .zip files.
"""
import os
import re
import time

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
    save_raw_file,
    save_raw_ndjson,
    save_state,
)

STATE_VERSION = 1
BASE_URL = "https://apps.bea.gov/api/data/"
REGIONAL_ZIP_BASE = "https://apps.bea.gov/regional/zip/"

# Per-spec wall-clock budget (seconds). Crawls that exceed it return cleanly
# with state advanced and resume next run. Sized so a sequential 12-spec run
# stays well inside any reasonable refresh window while still fetching a
# substantial slice of each dataset on the first pass. Operators can override
# via BEA_MAX_FETCH_SECONDS (read at call time).
MAX_FETCH_SECONDS = 90

# Documented BEA limit is 100 requests/minute per UserID (exceeding throttles
# the key for 1 hour). The connector DAG runs nodes sequentially, so this
# per-process limiter is effectively global. ~75/min leaves headroom.
RL_CALLS = 75
RL_PERIOD = 60

ENTITY_IDS = [
    "FixedAssets",
    "GDPbyIndustry",
    "IIP",
    "ITA",
    "InputOutput",
    "IntlServSTA",
    "IntlServTrade",
    "MNE",
    "NIPA",
    "NIUnderlyingDetail",
    "Regional",
    "UnderlyingGDPbyIndustry",
]

# MNE combos: enumerated up front (pure literal — no I/O). DirectionOfInvestment
# and Classification are both required and single-valued. Classification=Country
# with Country=all and SeriesID=0 ("all series") pulls every series for the
# direction in one call (~25-45k rows). Only one parameter may carry "all" per
# request (BEA "Invalid ALL parameter count"), so Industry is left unset.
_MNE_COMBOS = [
    (
        f"{direction}-Country",
        {
            "DirectionOfInvestment": direction,
            "Classification": "Country",
            "SeriesID": "0",
            "Country": "all",
            "Year": "all",
        },
    )
    for direction in ("outward", "inward", "parent")
]

# Driving-parameter iteration config per dataset. "enum" enumerates iter_param
# via GetParameterValues then issues one GetData per value with `fixed` params.
# Year token differs by dataset family: NIPA-style use "X" for all-years, the
# international/industry datasets use "ALL".
CONFIG = {
    "NIPA": {"strategy": "enum", "iter_param": "TableName",
             "fixed": {"Frequency": "A,Q,M", "Year": "X"}},
    "NIUnderlyingDetail": {"strategy": "enum", "iter_param": "TableName",
                           "fixed": {"Frequency": "A,Q,M", "Year": "X"}},
    "FixedAssets": {"strategy": "enum", "iter_param": "TableName",
                    "fixed": {"Year": "X"}},
    "InputOutput": {"strategy": "enum", "iter_param": "TableID",
                    "fixed": {"Year": "ALL"}},
    "GDPbyIndustry": {"strategy": "enum", "iter_param": "TableID",
                      "fixed": {"Industry": "ALL", "Frequency": "A,Q", "Year": "ALL"}},
    "UnderlyingGDPbyIndustry": {"strategy": "enum", "iter_param": "TableID",
                                "fixed": {"Industry": "ALL", "Frequency": "A,Q", "Year": "ALL"}},
    "ITA": {"strategy": "enum", "iter_param": "Indicator",
            "fixed": {"AreaOrCountry": "ALL", "Frequency": "ALL", "Year": "ALL"}},
    "IIP": {"strategy": "enum", "iter_param": "TypeOfInvestment",
            "fixed": {"Component": "ALL", "Frequency": "ALL", "Year": "ALL"}},
    "IntlServTrade": {"strategy": "enum", "iter_param": "TypeOfService",
                      "fixed": {"TradeDirection": "ALL", "Affiliation": "ALL",
                                "AreaOrCountry": "ALL", "Year": "ALL"}},
    # IntlServSTA requires exactly one Channel AND exactly one Industry per
    # call (error 331), so it crosses the data-bearing channels with every
    # industry. Non-data channels (AllChannels) just return 0 rows and skip.
    "IntlServSTA": {"strategy": "enum_cross", "iter_param": "Industry",
                    "cross_param": "Channel",
                    "cross_values": ["Mofas", "Mousas", "SalesMofas",
                                     "SalesMousas", "Trade",
                                     "UsExportsByMousas", "UsImportsFromMofas"],
                    "fixed": {"Destination": "ALL", "AreaOrCountry": "ALL",
                              "Year": "ALL"}},
    "MNE": {"strategy": "combos", "combos": _MNE_COMBOS},
    "Regional": {"strategy": "regional_zip"},
}

# Known-good BEA regional bulk ZIP codes (combined-series + per-table where the
# combined code isn't published). Validated live 2026-05-30: BEA returns an
# HTML error page (not a ZIP) for codes that don't exist, so each download is
# checked for the PK magic bytes and HTML responses are skipped — the list can
# stay generous without poisoning the crawl.
REGIONAL_ZIP_CODES = [
    "SAGDP", "SAINC", "SAPCE", "SARPP", "SASUMMARY",   # state annual
    "SQGDP", "SQINC",                                   # state quarterly
    "CAGDP1", "CAGDP2", "CAGDP8", "CAGDP9", "CAGDP11",  # county GDP (per-table)
    "CAINC1", "CAINC4", "CAINC5N", "CAINC6N",           # county income
    "CAINC30", "CAINC35", "CAINC45", "CAINC91",
    "CAEMP25N",                                          # county employment
]


# --------------------------------------------------------------------------- #
# HTTP transport: retry on transient, rate-limited, BEA error-envelope aware.
# --------------------------------------------------------------------------- #
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


class _BeaError(Exception):
    """A logical error returned inside a 200 response envelope (permanent)."""


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
@sleep_and_retry
@limits(calls=RL_CALLS, period=RL_PERIOD)
def _http_get(url: str, params: dict | None) -> httpx.Response:
    resp = get(url, params=params, timeout=(15.0, 240.0))
    resp.raise_for_status()
    return resp


def _api_key() -> str:
    key = os.environ.get("BEA_API_KEY") or os.environ.get("BEA_USER_ID")
    if not key:
        raise RuntimeError("BEA_API_KEY (or BEA_USER_ID) env var is not set")
    return key


def _bea_call(method: str, **params) -> dict:
    """Issue a BEA API method call, returning the Results dict.

    Raises _BeaError for the BEA error envelope (Results.Error or top-level
    Error), which is a logical/permanent failure carried inside a 200.
    """
    query = {"UserID": _api_key(), "ResultFormat": "JSON", "method": method, **params}
    resp = _http_get(BASE_URL, query)
    api = resp.json().get("BEAAPI", {})
    err = api.get("Error")
    if err:
        raise _BeaError(f"{method}: {err}")
    res = api.get("Results")
    if isinstance(res, list):
        res = res[0] if res else {}
    if isinstance(res, dict):
        inner = res.get("Error")
        if inner:
            raise _BeaError(f"{method}: {inner}")
        return res
    return {}


def _enumerate_values(dataset: str, iter_param: str) -> list[str]:
    res = _bea_call("GetParameterValues", datasetname=dataset, ParameterName=iter_param)
    vals = res.get("ParamValue", [])
    if isinstance(vals, dict):
        vals = [vals]
    out = []
    for v in vals:
        raw = v.get(iter_param)
        if raw is None:
            raw = v.get("Key")
        if raw is not None:
            out.append(str(raw))
    return out


def _get_data(dataset: str, params: dict) -> list[dict]:
    res = _bea_call("GetData", datasetname=dataset, **params)
    data = res.get("Data") or []
    if isinstance(data, dict):
        data = [data]
    return data


_SAFE_RE = re.compile(r"[^A-Za-z0-9]+")


def _safe_key(batch_key: str) -> str:
    return _SAFE_RE.sub("_", batch_key).strip("_")


def _eid_lower(entity_id: str) -> str:
    return entity_id.lower().replace("_", "-")


def _budget_seconds() -> float:
    try:
        return float(os.environ.get("BEA_MAX_FETCH_SECONDS", MAX_FETCH_SECONDS))
    except (TypeError, ValueError):
        return float(MAX_FETCH_SECONDS)


# --------------------------------------------------------------------------- #
# Fetch
# --------------------------------------------------------------------------- #
def fetch_dataset(entity_id: str) -> None:
    cfg = CONFIG[entity_id]
    if cfg["strategy"] == "regional_zip":
        _fetch_regional(entity_id)
        return

    asset_base = f"bea-{_eid_lower(entity_id)}"
    state = load_state(asset_base)
    if state.get("schema_version") != STATE_VERSION:
        if state:
            print(f"{asset_base}: state schema_version != {STATE_VERSION}, resetting", flush=True)
        state = {}

    if cfg["strategy"] == "enum":
        work = [
            (value, {cfg["iter_param"]: value, **cfg["fixed"]})
            for value in _enumerate_values(entity_id, cfg["iter_param"])
        ]
    elif cfg["strategy"] == "enum_cross":
        inner = _enumerate_values(entity_id, cfg["iter_param"])
        work = [
            (f"{cross}-{value}",
             {cfg["cross_param"]: cross, cfg["iter_param"]: value, **cfg["fixed"]})
            for cross in cfg["cross_values"]
            for value in inner
        ]
    else:  # combos
        work = list(cfg["combos"])

    all_keys = {bk for bk, _ in work}
    completed = set(state.get("completed", []))
    # Crawl finished on a prior run -> start a fresh epoch and re-fetch (the
    # only way to pick up BEA's scheduled revisions, since there's no since=).
    if completed and completed >= all_keys:
        print(f"{asset_base}: crawl complete ({len(all_keys)} batches) -> new epoch", flush=True)
        completed = set()

    deadline = time.monotonic() + _budget_seconds()
    total_rows = 0
    batches_written = 0
    for batch_key, params in work:
        if batch_key in completed:
            continue
        if time.monotonic() > deadline:
            print(f"{asset_base}: budget hit ({len(completed)}/{len(all_keys)} done), "
                  f"resuming next run", flush=True)
            break
        asset = f"{asset_base}-{_safe_key(batch_key)}"
        try:
            rows = _get_data(entity_id, params)
        except _BeaError as exc:
            # Logical error for this slice (e.g. "no data found", bad combo):
            # mark done and move on so the crawl makes progress.
            print(f"{asset}: skipping ({exc})", flush=True)
            completed.add(batch_key)
            _save_progress(asset_base, completed, len(all_keys), total_rows, batches_written)
            continue
        if rows:
            save_raw_ndjson(rows, asset)  # raw FIRST, then state
            total_rows += len(rows)
            batches_written += 1
        completed.add(batch_key)
        _save_progress(asset_base, completed, len(all_keys), total_rows, batches_written)

    print(f"{asset_base}: wrote {total_rows:,} rows in {batches_written} batches this run "
          f"({len(completed)}/{len(all_keys)} batches done)", flush=True)


def _fetch_regional(entity_id: str) -> None:
    asset_base = f"bea-{_eid_lower(entity_id)}"
    state = load_state(asset_base)
    if state.get("schema_version") != STATE_VERSION:
        if state:
            print(f"{asset_base}: state schema_version != {STATE_VERSION}, resetting", flush=True)
        state = {}

    all_keys = set(REGIONAL_ZIP_CODES)
    completed = set(state.get("completed", []))
    if completed and completed >= all_keys:
        print(f"{asset_base}: crawl complete ({len(all_keys)} zips) -> new epoch", flush=True)
        completed = set()

    deadline = time.monotonic() + _budget_seconds()
    saved = 0
    total_bytes = 0
    for code in REGIONAL_ZIP_CODES:
        if code in completed:
            continue
        if time.monotonic() > deadline:
            print(f"{asset_base}: budget hit ({len(completed)}/{len(all_keys)} done), "
                  f"resuming next run", flush=True)
            break
        url = f"{REGIONAL_ZIP_BASE}{code}.zip"
        resp = _http_get(url, None)
        content = resp.content
        if content[:2] != b"PK":
            # BEA serves an HTML error page (200) for unpublished codes.
            print(f"{asset_base}-{code}: not a ZIP "
                  f"(content-type {resp.headers.get('content-type')}), skipping", flush=True)
            completed.add(code)
            _save_progress(asset_base, completed, len(all_keys), saved, saved, key="zips")
            continue
        save_raw_file(content, f"{asset_base}-{code}", extension="zip")
        saved += 1
        total_bytes += len(content)
        completed.add(code)
        _save_progress(asset_base, completed, len(all_keys), total_bytes, saved, key="zips")

    print(f"{asset_base}: saved {saved} regional ZIPs ({total_bytes:,} bytes) this run "
          f"({len(completed)}/{len(all_keys)} done)", flush=True)


def _save_progress(asset_base, completed, total, records, batches, key="records"):
    save_state(asset_base, {
        "schema_version": STATE_VERSION,
        "completed": sorted(completed),
        "total": total,
        "last_run_stats": {key: records, "batches": batches},
    })


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bea-{_eid_lower(eid)}",
        fn=fetch_dataset,
        args=(eid,),
        deps=(),
        kind="download",
    )
    for eid in ENTITY_IDS
]
