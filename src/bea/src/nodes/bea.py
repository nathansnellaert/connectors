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

Per-dataset GetData strategy (parameter values discovered live via
GetParameterValues; never a hardcoded year range). The all-value token for a
parameter is dataset-specific (e.g. NIPA/GDPbyIndustry/GDP-by-industry use
"ALL"; IIP/ITA/IntlServSTA/IntlServTrade use "All" for member params with
Year="ALL"). These were verified live 2026-05-30:
  - single        : one (or few) GetData calls with all-valued parameters
                    (GDPbyIndustry, IntlServSTA — small enough all-at-once).
  - loop_tablename: GetParameterValues(TableName) then one GetData per table
                    (NIPA, NIUnderlyingDetail, FixedAssets).
  - loop_tableid  : GetParameterValues(TableID) then one GetData per table id
                    (InputOutput).
  - loop_tableid_gdp: GDPbyIndustry-family per-table sweep — one GetData per
                    (TableID, frequency) with Industry=ALL/Year=ALL
                    (UnderlyingGDPbyIndustry: the all-tables request 204s
                    "Error retrieving GDP by Industry data"; per-table calls
                    succeed — verified TableID=210 -> 5348 rows).
  - loop_param    : one GetData per value of a single enumerated parameter,
                    everything else all-valued (IntlServTrade — the API silently
                    returns 0 rows for ALL TypeOfService + ALL AreaOrCountry, so
                    pin one TypeOfService per call with TradeDirection/
                    Affiliation/AreaOrCountry="All", Year="ALL"; verified live:
                    one service returns ~4.9k rows).
  - loop_year     : one GetData per Year, all frequencies in one call (IIP —
                    GetData rejects TypeOfInvestment=All together with Year=ALL
                    ["exactly one TypeOfInvestment OR one Year"], so pin Year and
                    keep TypeOfInvestment/Component all-valued; verified Year=2024
                    -> 5161 rows; ~50 years).
  - loop_area     : one GetData per AreaOrCountry value (ITA — API forbids ALL
                    Indicator + ALL AreaOrCountry together).
  - mne           : DirectionOfInvestment x Classification x Year. MNE GetData
                    REQUIRES SeriesID/OwnershipLevel/NonbankAffiliatesOnly (the
                    prior omission caused APIErrorCode 40 on every combo); with
                    SeriesID=0/OwnershipLevel=0/NonbankAffiliatesOnly=0 a single
                    (direction, classification, year) returns real rows
                    (verified: outward/Country/2021 ~152k rows). We loop per
                    year to bound per-call memory and skip invalid
                    (direction, classification) combos after a single probe so
                    the permanent-error rate stays well under BEA's 30/min cap.
  - regional_zip  : per-series ZIP bulk export.

Transient envelope errors: BEA returns APIErrorCode 204 on a COLD/large GetData
("result is being generated, retry") — verified live: an all-services
IntlServTrade call returned 204 on the first hit and the full payload on retry.
204 (and any "try again"/"being generated" description) is therefore treated as
TRANSIENT and retried by the tenacity backoff inside `_api`, NOT skipped — the
prior code skipped all envelope errors, which would silently drop whole tables
on a fresh CI run. Permanent envelope errors (e.g. 40 = invalid param combo)
are surfaced to the caller, which skips that single call (error-paced).

Rate / bandwidth limits (documented per UserID, GLOBAL across processes):
100 requests/min, 100MB/min, 30 errors/min; exceeding any throttles the key for
1 hour. The DAG runs nodes sequentially (DAG_PARALLELISM=1) so a per-process
cap of 80 req/min (~0.75s spacing) keeps requests under the ceiling, a rolling
80MB/min byte budget keeps bandwidth under it, and a 2.5s sleep after each
permanent envelope error keeps the error rate under 30/min.

Raw format: NDJSON (gzip-streamed) for REST datasets — row shapes differ across
datasets and carry optional footnote columns, so NDJSON avoids a brittle parquet
schema and bounds memory to one GetData response at a time. Regional ZIPs are
opaque bytes saved per series.
"""
import json
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

# Regional bulk-export series ZIPs. Verified live: county/metro series are split
# per table-number (CAINC1, CAGDP2, ...) and the bare codes from the research
# handoff (CAINC/SAEMP/SAGDS) do NOT exist as ZIPs — they return BEA's HTML
# landing page, which the non-ZIP guard in _fetch_regional skips. This is the
# working set covering state + quarterly + county GDP/income/PCE. Any code that
# later 404s or returns HTML is skipped, not fatal.
REGIONAL_SERIES = [
    "SAGDP", "SAINC", "SAPCE", "SARPP",                  # state annual
    "SQGDP", "SQINC",                                    # state quarterly
    "CAGDP1", "CAGDP2", "CAGDP8", "CAGDP9", "CAGDP11",   # county GDP
    "CAINC1", "CAINC4", "CAINC5N", "CAINC5S", "CAINC6N",  # county income
    "CAINC30", "CAINC35", "CAINC45",
]

# Per-dataset GetData driving strategy. The all-value tokens below are the exact
# AllValue strings BEA reports in GetParameterList for each (dataset, parameter),
# verified live 2026-05-30.
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
        # The all-at-once request (TableID=ALL + Industry=ALL + Year=ALL) is too
        # large for this dataset: BEA returns APIErrorCode 204 "Error retrieving
        # GDP by Industry data" and never completes it, even on retry (verified —
        # this was the prior failure; the smaller sibling GDPbyIndustry succeeds
        # all-at-once). Split per TableID instead. Verified: TableID=210 +
        # Industry=ALL + Year=ALL + Frequency=A returned 5348 rows.
        "mode": "loop_tableid_gdp",
        "freqs": ["A", "Q"],
    },
    "IntlServTrade": {
        # The AllValue for EVERY IntlServTrade parameter is the literal "ALL"
        # (verified live via GetParameterList 2026-05-30); the earlier tokens
        # "AllTradeDirections"/"AllAffiliations"/"AllCountries" are specific
        # member values, not all-selectors, so the API accepted them but matched
        # zero rows (the prior run logged 0 rows across all 117 calls, 0 errors).
        # The dataset has no Frequency parameter. Pin one TypeOfService per call
        # (its all-token is also "ALL", but the all-services row is an aggregate
        # we get by looping every service) with the other three params all-valued.
        # Verified: TypeOfService=AccountAuditBookkeep + TradeDirection=All +
        # Affiliation=All + AreaOrCountry=All + Year=ALL returned 4888 rows.
        "mode": "loop_param",
        "param": "TypeOfService",
        "base": {
            "TradeDirection": "All",
            "Affiliation": "All",
            "AreaOrCountry": "All",
            "Year": "ALL",
        },
    },
    "IntlServSTA": {
        "mode": "single",
        "base": {
            "Channel": "All",
            "Destination": "All",
            "Industry": "All",
            "AreaOrCountry": "AllCountries",
            "Year": "ALL",
        },
    },
    "IIP": {
        # IIP GetData rejects TypeOfInvestment=All together with Year=ALL:
        # "Either exactly one TypeOfInvestment must be requested or exactly one
        # Year must be requested" (verified live — this killed all 3 prior
        # calls). Satisfy the rule by pinning one Year per call and keeping
        # TypeOfInvestment=All/Component=All, with all frequencies in one
        # request. Verified live: Year=2024 + TypeOfInvestment=All +
        # Component=All + Frequency=A,QNSA,QSA -> 5161 rows. IIP exposes ~50
        # years, so this is ~50 calls — far cheaper than looping the 399
        # TypeOfInvestment values (which also works, 188 rows/call, but is 1197
        # calls).
        "mode": "loop_year",
        "base": {"TypeOfInvestment": "All", "Component": "All"},
        "freq": "A,QNSA,QSA",
    },
    "ITA": {
        "mode": "loop_area",
        # API forbids ALL Indicator + ALL AreaOrCountry together; AreaOrCountry
        # has the fewer values, so loop it and keep Indicator all-valued.
        "base": {"Indicator": "All"},
        "freq": "A,QNSA,QSA",
    },
    "MNE": {"mode": "mne"},
    "Regional": {"mode": "regional_zip"},
}

# spec id -> original dataset name (entity ids have no underscores).
ID_TO_DATASET = {f"bea-{e.lower()}": e for e in ENTITY_IDS}

# Sleep after each PERMANENT envelope error so the error rate stays under BEA's
# documented 30-errors/minute throttle (60/30 = 2.0s floor; 2.5s for slack).
_ERROR_SLEEP = 2.5

# BEA envelope error codes that are TRANSIENT (cold/large result still being
# generated). Retried by the backoff loop rather than skipped.
_RETRYABLE_API_CODES = {"204"}
_RETRYABLE_DESC_HINTS = ("being generated", "try again", "please try", "retrieving")

# Rolling byte budget: 80% of BEA's documented 100MB/min, per UserID.
_BYTE_BUDGET = int(0.8 * 100 * 1024 * 1024)
_byte_window: list = []  # list[(timestamp, nbytes)] within the trailing 60s


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


class _BeaRetry(Exception):
    """Raised on a retryable BEA envelope error so tenacity retries the call."""


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _BeaRetry):
        return True
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


def _throttle_bytes(nbytes: int) -> None:
    """Keep a trailing-60s byte total under BEA's documented 100MB/min."""
    now = time.time()
    cutoff = now - 60.0
    while _byte_window and _byte_window[0][0] < cutoff:
        _byte_window.pop(0)
    if _byte_window and sum(b for _, b in _byte_window) + nbytes > _BYTE_BUDGET:
        time.sleep(max(0.0, 60.0 - (now - _byte_window[0][0])))
        now2 = time.time()
        cutoff2 = now2 - 60.0
        while _byte_window and _byte_window[0][0] < cutoff2:
            _byte_window.pop(0)
    _byte_window.append((time.time(), nbytes))


def _envelope_error(js: dict):
    """Return the BEA error dict (or None). Error may live at BEAAPI.Error or
    BEAAPI.Results.Error, and either may itself be a single-element list."""
    top = js.get("BEAAPI", js) if isinstance(js, dict) else {}
    res = top.get("Results")
    if isinstance(res, list):
        res = res[0] if res else {}
    err = None
    if isinstance(res, dict) and res.get("Error"):
        err = res["Error"]
    elif top.get("Error"):
        err = top["Error"]
    if isinstance(err, list):
        err = err[0] if err else None
    return err if isinstance(err, dict) else None


@sleep_and_retry
@limits(calls=80, period=60)  # ~80% of documented 100 req/min, per UserID (global)
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _api(params: dict) -> dict:
    """One BEA REST call. Returns the parsed JSON envelope. Raises `_BeaRetry`
    on a transient envelope error so the backoff loop retries; permanent
    envelope errors are returned for the caller to handle."""
    key = os.environ["BEA_API_KEY"]
    full = {"UserID": key, "ResultFormat": "JSON", **params}
    resp = get(BASE_URL, params=full, timeout=(10.0, 240.0))
    resp.raise_for_status()
    _throttle_bytes(len(resp.content))
    js = resp.json()
    err = _envelope_error(js)
    if err is not None:
        code = str(err.get("APIErrorCode"))
        desc = str(err.get("APIErrorDescription") or "")
        if code in _RETRYABLE_API_CODES or any(h in desc.lower() for h in _RETRYABLE_DESC_HINTS):
            raise _BeaRetry(f"BEA {code}: {desc[:140]}")
    return js


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
    _throttle_bytes(len(resp.content))
    return resp.content


def _results_data(js: dict):
    """Pull (Data rows, permanent-error) out of the BEAAPI envelope. Transient
    errors are already retried/raised inside `_api`, so any error here is
    permanent for this call."""
    top = js.get("BEAAPI", js)
    res = top.get("Results")
    if isinstance(res, list):
        res = res[0] if res else {}
    err = _envelope_error(js)
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
    if mode == "loop_param":
        param = strat["param"]
        return [
            {**strat["base"], param: code}
            for code in _param_codes(dataset, param)
        ]
    if mode == "loop_year":
        # One GetData per Year (all frequencies in a single call), used when the
        # API forbids an all-valued param alongside Year=ALL (IIP): pin Year and
        # keep the other params all-valued.
        return [
            {**strat["base"], "Year": y, "Frequency": strat["freq"]}
            for y in _param_codes(dataset, "Year")
        ]
    if mode == "loop_tableid_gdp":
        # GDPbyIndustry-family per-table sweep: one GetData per (TableID, freq)
        # with Industry=ALL/Year=ALL, used when the all-tables request is too big
        # and 204s (UnderlyingGDPbyIndustry).
        return [
            {"TableID": code, "Industry": "ALL", "Year": "ALL", "Frequency": f}
            for code in _param_codes(dataset, "TableID")
            for f in strat["freqs"]
        ]
    if mode == "loop_area":
        areas = [a for a in _param_codes(dataset, "AreaOrCountry") if a != "AllCountries"]
        return [
            {**strat["base"], "AreaOrCountry": a, "Frequency": strat["freq"], "Year": "ALL"}
            for a in areas
        ]
    raise ValueError(f"{dataset}: no call planner for mode {mode!r}")


def _write_rows(fh, rows) -> int:
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
            try:
                js = _api({"method": "GetData", "datasetname": dataset, **params})
            except _BeaRetry as e:
                # A transient envelope error (e.g. 204 "being generated") that
                # never resolved across the full backoff. Skip this single call
                # rather than failing the whole dataset; error-paced like a
                # permanent envelope error so we stay under BEA's 30/min cap.
                errors += 1
                print(f"  [{dataset}] persistent transient error on {params}: {e}", flush=True)
                time.sleep(_ERROR_SLEEP)
                continue
            data, err = _results_data(js)
            if err:
                errors += 1
                print(
                    f"  [{dataset}] envelope error on {params}: "
                    f"{err.get('APIErrorDescription') or err}",
                    flush=True,
                )
                time.sleep(_ERROR_SLEEP)  # keep permanent-error rate < 30/min
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
    """MNE: one GetData per (DirectionOfInvestment, Classification), Year=all.

    MNE GetData requires SeriesID/OwnershipLevel/NonbankAffiliatesOnly (omitting
    them returns APIErrorCode 40 — the prior failure). With SeriesID=0/
    OwnershipLevel=0/NonbankAffiliatesOnly=0, a single Year="all" call per pair
    returns the full history and stays small (verified ~31k rows/12MB for the
    largest pair), so no per-year loop is needed. Invalid (direction,
    classification) combos return a permanent envelope error and are skipped
    (error-paced under BEA's 30-errors/minute cap).
    """
    dataset = "MNE"
    base = {"SeriesID": "0", "OwnershipLevel": "0", "NonbankAffiliatesOnly": "0"}
    directions = _param_codes(dataset, "DirectionOfInvestment")
    classifications = _param_codes(dataset, "Classification")
    total = 0
    errors = 0
    pairs_ok = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as fh:
        for direction in directions:
            for classification in classifications:
                # Year="all" returns the full history in one call (verified
                # ~31k rows/12MB for the largest pair). The Year parameter list
                # itself contains "all" PLUS every individual year, so iterating
                # it would re-fetch the whole history and duplicate every row —
                # one Year="all" call per pair is both complete and correct.
                data, err = _results_data(_api({
                    "method": "GetData",
                    "datasetname": dataset,
                    "DirectionOfInvestment": direction,
                    "Classification": classification,
                    "Year": "all",
                    **base,
                }))
                if err:
                    errors += 1
                    time.sleep(_ERROR_SLEEP)
                    print(
                        f"  [MNE] {direction}/{classification}: invalid combo "
                        f"({err.get('APIErrorCode')}), skipped",
                        flush=True,
                    )
                    continue
                pair_rows = _write_rows(fh, data)
                pairs_ok += 1
                total += pair_rows
                print(
                    f"  [MNE] {direction}/{classification}: {pair_rows} rows "
                    f"(running total {total}, {pairs_ok} valid pairs)",
                    flush=True,
                )
    if total == 0:
        raise RuntimeError(
            f"MNE: 0 rows across {len(directions)}x{len(classifications)} combos "
            f"({errors} errors)"
        )
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
