"""BEA (Bureau of Economic Analysis) download node.

Catalog connector: one NodeSpec per BEA dataset (the entity union). Every
dataset is fetched through the BEA REST API at https://apps.bea.gov/api/data/
EXCEPT `Regional`, which is pulled as persistent per-series ZIP bulk exports —
driving Regional via REST would require a GeoFips x LineCode x TableName
cartesian of hundreds-of-thousands of calls (see research handoff).

Fetch shape: stateless full re-pull (download prompt shape 1). BEA exposes no
`since`/`modifiedAfter`/cursor on any method, so every refresh re-fetches the
whole corpus and overwrites. Revisions/late corrections are picked up for free.
Freshness gating is the maintain step's job, not ours.

Per-dataset GetData strategy (discovered live via GetParameterValues; never a
hardcoded year range):
  - single        : one (or few) GetData calls with ALL-valued parameters
                    (GDPbyIndustry, UnderlyingGDPbyIndustry, IntlServTrade,
                     IntlServSTA).
  - loop_tablename: GetParameterValues(TableName) then one GetData per table
                    (NIPA, NIUnderlyingDetail, FixedAssets).
  - loop_tableid  : GetParameterValues(TableID) then one GetData per table id
                    (InputOutput).
  - loop_year     : one GetData per year, all TypeOfInvestment/Component
                    (IIP — API forbids more than one ALL among
                     TypeOfInvestment/Year, so we pin Year).
  - loop_area     : one GetData per AreaOrCountry (except AllCountries),
                    Indicator=ALL (ITA — API forbids ALL Indicator + ALL
                    AreaOrCountry together; AreaOrCountry has fewer values).
  - mne           : DirectionOfInvestment x Classification, Year=all, with a
                    single breakout dimension set to "all" (MNE caps ALL-valued
                    params at 3; invalid combos return an envelope error and are
                    skipped).
  - regional_zip  : per-series ZIP bulk export.

Rate limit: BEA documents 100 requests/min PER UserID (global across processes,
not per-process). The DAG runs nodes sequentially by default (DAG_PARALLELISM=1),
so a per-process cap of 80/min (~0.75s spacing) keeps the global rate under the
documented ceiling. 429 means the key is throttled for 1 hour — retries will
exhaust and surface it as a spec failure rather than silently spinning.

Raw format: NDJSON (gzip-streamed) for REST datasets — row shapes differ across
datasets and carry optional footnote columns, so NDJSON avoids a brittle parquet
schema and bounds memory to one GetData response at a time. Regional ZIPs are
opaque bytes saved per series.
"""
import os
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
    save_raw_file,
    raw_writer,
    save_state,
    list_raw_files,
)

# --- entity union (authoritative; one spec per entry) ----------------------
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

BASE_URL = "https://apps.bea.gov/api/data/"
REGIONAL_ZIP_URL = "https://apps.bea.gov/regional/zip/{series}.zip"

# Regional bulk-export series ZIPs. Verified live 2026-05-30: county/metro
# series are split per table-number (CAINC1, CAGDP2, ...) and the bare codes
# from the research handoff (CAINC/SAEMP/SAGDS) do NOT exist as ZIPs — they
# return BEA's HTML landing page, which the non-ZIP guard in _fetch_regional
# skips. This is the verified working set covering state + quarterly + county
# GDP/income/PCE. Any code that later 404s or returns HTML is skipped, not fatal.
REGIONAL_SERIES = [
    "SAGDP", "SAINC", "SAPCE", "SARPP",          # state annual
    "SQGDP", "SQINC",                            # state quarterly
    "CAGDP1", "CAGDP2", "CAGDP8", "CAGDP9", "CAGDP11",   # county GDP
    "CAINC1", "CAINC4", "CAINC5N", "CAINC5S", "CAINC6N",  # county income
    "CAINC30", "CAINC35", "CAINC45",
]

# Per-dataset GetData driving strategy. `base` params are merged into every call.
DATASET_STRATEGY = {
    "NIPA": {"mode": "loop_tablename", "freq": "A,Q,M", "year": "ALL"},
    "NIUnderlyingDetail": {"mode": "loop_tablename", "freq": "A,Q,M", "year": "ALL"},
    "FixedAssets": {"mode": "loop_tablename", "freq": None, "year": "ALL"},
    "InputOutput": {"mode": "loop_tableid", "year": "ALL"},
    "GDPbyIndustry": {
        "mode": "single",
        "base": {"TableID": "ALL", "Industry": "ALL", "Year": "ALL"},
        "freqs": ["A", "Q"],
    },
    "UnderlyingGDPbyIndustry": {
        "mode": "single",
        "base": {"TableID": "ALL", "Industry": "ALL", "Year": "ALL"},
        "freqs": ["A", "Q"],
    },
    "IntlServTrade": {
        "mode": "single",
        "base": {
            "TypeOfService": "ALL",
            "TradeDirection": "ALL",
            "Affiliation": "ALL",
            "AreaOrCountry": "AllCountries",
            "Year": "ALL",
        },
    },
    "IntlServSTA": {
        "mode": "single",
        "base": {
            "Channel": "ALL",
            "Destination": "ALL",
            "Industry": "ALL",
            "AreaOrCountry": "AllCountries",
            "Year": "ALL",
        },
    },
    "IIP": {
        "mode": "loop_year",
        "base": {"TypeOfInvestment": "ALL", "Component": "ALL"},
        "freq": "A,QSA,QNSA",
    },
    "ITA": {
        "mode": "loop_area",
        "base": {"Indicator": "ALL"},
        "freq": "A,QSA,QNSA",
    },
    "MNE": {"mode": "mne"},
    "Regional": {"mode": "regional_zip"},
}

# spec id -> original dataset name (entity ids have no underscores).
ID_TO_DATASET = {f"bea-{e.lower()}": e for e in ENTITY_IDS}

# MNE breakout dimensions tried in order; first that yields data wins. MNE caps
# ALL-valued params at 3, so exactly one breakout is set to "all" per call.
_MNE_BREAKOUTS = [{"Country": "all"}, {"Industry": "all"}, {"State": "all"}, {}]


# --- transport -------------------------------------------------------------
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
@limits(calls=80, period=60)  # ~80% of documented 100/min, per UserID (global)
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _api(params: dict) -> dict:
    """One BEA REST call. Returns the parsed JSON envelope."""
    key = os.environ["BEA_API_KEY"]
    full = {"UserID": key, "ResultFormat": "JSON", **params}
    resp = get(BASE_URL, params=full, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp.json()


@sleep_and_retry
@limits(calls=80, period=60)
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _api_bytes(url: str) -> bytes:
    resp = get(url, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp.content


def _results_data(js: dict):
    """Pull (Data rows, Error) out of the BEAAPI envelope.

    Results may be a dict or a single-element list, and an error can surface
    either at BEAAPI.Error or BEAAPI.Results.Error — check both.
    """
    top = js.get("BEAAPI", js)
    err = top.get("Error")
    res = top.get("Results")
    if isinstance(res, list):
        res = res[0] if res else {}
    if isinstance(res, dict) and res.get("Error"):
        err = res["Error"]
    data = res.get("Data", []) if isinstance(res, dict) else []
    return data, err


def _extract_code(item: dict, pname: str):
    for k in (pname, pname + "ID", "Key", "key"):
        if k in item:
            return item[k]
    return None


def _param_codes(dataset: str, pname: str) -> list[str]:
    """GetParameterValues -> list of code strings for `pname`."""
    js = _api(
        {"method": "GetParameterValues", "datasetname": dataset, "ParameterName": pname}
    )
    res = js.get("BEAAPI", {}).get("Results")
    if isinstance(res, list):
        res = res[0] if res else {}
    vals = res.get("ParamValue", []) if isinstance(res, dict) else []
    out = []
    for v in vals:
        code = _extract_code(v, pname)
        if code is not None and str(code) != "":
            out.append(str(code))
    if not out:
        raise RuntimeError(f"{dataset}.{pname}: GetParameterValues returned no values")
    return out


# --- call planners ---------------------------------------------------------
def _plan_calls(dataset: str, strat: dict) -> list[dict]:
    """Return the list of GetData param-dicts (excluding method/datasetname)."""
    mode = strat["mode"]
    if mode == "single":
        base = strat["base"]
        freqs = strat.get("freqs")
        if freqs:
            return [{**base, "Frequency": f} for f in freqs]
        return [dict(base)]
    if mode == "loop_tablename":
        calls = []
        for code in _param_codes(dataset, "TableName"):
            params = {"TableName": code, "Year": strat["year"]}
            if strat.get("freq"):
                params["Frequency"] = strat["freq"]
            calls.append(params)
        return calls
    if mode == "loop_tableid":
        return [
            {"TableID": code, "Year": strat["year"]}
            for code in _param_codes(dataset, "TableID")
        ]
    if mode == "loop_year":
        return [
            {**strat["base"], "Frequency": strat["freq"], "Year": y}
            for y in _param_codes(dataset, "Year")
        ]
    if mode == "loop_area":
        areas = [a for a in _param_codes(dataset, "AreaOrCountry") if a != "AllCountries"]
        return [
            {**strat["base"], "AreaOrCountry": a, "Frequency": strat["freq"], "Year": "ALL"}
            for a in areas
        ]
    raise ValueError(f"{dataset}: no call planner for mode {mode!r}")


def _write_rows(fh, rows) -> int:
    import json

    n = 0
    for row in rows:
        fh.write(json.dumps(row, ensure_ascii=False))
        fh.write("\n")
        n += 1
    return n


# --- fetchers --------------------------------------------------------------
def _fetch_planned(asset: str, dataset: str, strat: dict) -> int:
    """Standard REST datasets: plan calls, stream Data rows to NDJSON."""
    calls = _plan_calls(dataset, strat)
    total = 0
    errors = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as fh:
        for i, params in enumerate(calls, 1):
            js = _api({"method": "GetData", "datasetname": dataset, **params})
            data, err = _results_data(js)
            if err:
                errors += 1
                print(
                    f"  [{dataset}] envelope error on {params}: "
                    f"{err.get('APIErrorDescription') or err}",
                    flush=True,
                )
                continue
            total += _write_rows(fh, data)
            if i % 25 == 0 or i == len(calls):
                print(
                    f"  [{dataset}] call {i}/{len(calls)}: {total} rows so far",
                    flush=True,
                )
    if total == 0:
        raise RuntimeError(
            f"{dataset}: 0 rows across {len(calls)} GetData calls ({errors} errors)"
        )
    return total


def _fetch_mne(asset: str) -> int:
    """MNE: DirectionOfInvestment x Classification, Year=all, one breakout=all."""
    dataset = "MNE"
    directions = _param_codes(dataset, "DirectionOfInvestment")
    classifications = _param_codes(dataset, "Classification")
    total = 0
    combos = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as fh:
        for direction in directions:
            for classification in classifications:
                combos += 1
                got = False
                for breakout in _MNE_BREAKOUTS:
                    params = {
                        "method": "GetData",
                        "datasetname": dataset,
                        "DirectionOfInvestment": direction,
                        "Classification": classification,
                        "Year": "all",
                        **breakout,
                    }
                    js = _api(params)
                    data, err = _results_data(js)
                    if err or not data:
                        continue
                    total += _write_rows(fh, data)
                    got = True
                    break
                if not got:
                    print(
                        f"  [MNE] no data for {direction}/{classification} "
                        f"(all breakouts invalid) - skipping",
                        flush=True,
                    )
                if combos % 10 == 0:
                    print(f"  [MNE] {combos} combos, {total} rows so far", flush=True)
    if total == 0:
        raise RuntimeError("MNE: 0 rows across all direction/classification combos")
    return total


def _fetch_regional(asset: str) -> int:
    """Regional: download persistent per-series ZIP bulk exports."""
    saved = 0
    for series in REGIONAL_SERIES:
        url = REGIONAL_ZIP_URL.format(series=series)
        try:
            content = _api_bytes(url)
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if 400 <= code < 500 and code != 429:
                print(f"  [Regional] {series}.zip -> HTTP {code}, skipping", flush=True)
                continue
            raise
        if not content.startswith(b"PK"):
            print(
                f"  [Regional] {series}.zip not a zip ({content[:8]!r}), skipping",
                flush=True,
            )
            continue
        save_raw_file(content, f"{asset}-{series.lower()}", extension="zip")
        saved += 1
        print(f"  [Regional] saved {series}.zip ({len(content)} bytes)", flush=True)
    if saved == 0:
        raise RuntimeError("Regional: no series ZIPs downloaded")
    return saved


def fetch_one(node_id: str) -> None:
    """Fetch a single BEA dataset. The runtime passes the spec id; it IS the
    asset name to write. Recovers the dataset from the id and dispatches on the
    per-dataset strategy. Stateless full re-pull — no freshness short-circuit."""
    dataset = ID_TO_DATASET[node_id]
    strat = DATASET_STRATEGY[dataset]
    mode = strat["mode"]
    started = time.time()

    if mode == "regional_zip":
        count = _fetch_regional(node_id)
        unit = "zips"
    elif mode == "mne":
        count = _fetch_mne(node_id)
        unit = "rows"
    else:
        count = _fetch_planned(node_id, dataset, strat)
        unit = "rows"

    elapsed = round(time.time() - started, 1)
    print(f"[{dataset}] done: {count} {unit} in {elapsed}s", flush=True)
    # Observability only — not a watermark. Stateless re-pull ignores this on
    # the next run; it just lets later runs diff record counts.
    save_state(node_id, {"last_run_stats": {unit: count, "elapsed_s": elapsed}})


DOWNLOAD_SPECS = [
    NodeSpec(id=f"bea-{eid.lower().replace('_', '-')}", fn=fetch_one, kind="download")
    for eid in ENTITY_IDS
]
