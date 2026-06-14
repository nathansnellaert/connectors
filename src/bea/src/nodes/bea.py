"""BEA connector — Bureau of Economic Analysis REST API.

One download node per BEA dataset (the 12-entity union). Each node walks the
dataset's catalog (GetParameterValues / GetParameterValuesFiltered) and drives
GetData, streaming every observation to a single gzip ndjson raw asset. A SQL
transform per dataset then projects/casts that raw into a published Delta table.

Access strategy (per research handoff): REST only, base
https://apps.bea.gov/api/data/. Auth is a free 36-char UserID passed as the
`UserID` query param, read from BEA_API_KEY. Every response is wrapped in
{BEAAPI:{Request, Results|Error}}; Results may itself carry an Error, so error
detection checks the JSON body, never just HTTP status.

Fetch shape: STATELESS FULL RE-PULL. BEA exposes no since/modifiedAfter filter
on any method, so every refresh re-fetches the full corpus and overwrites.
Revisions/late corrections are picked up for free. The maintain step (authored
later) gates whether a node runs on a given refresh; this module never
short-circuits on freshness.

Rate limit (documented, per UserID): 100 req/min, 100MB/min, 30 errors/min;
exceeding any throttles the key for 1h. The DAG runs nodes sequentially by
default (DAG_PARALLELISM=1), so one process owns the budget — we pace to
~55 req/min (well under 100) and let exponential backoff absorb any 429.

Per-dataset GetData driving (each dataset has a different parameter surface,
discovered live 2026-06-14):
  - NIPA / NIUnderlyingDetail : enumerate TableName, Frequency="A,Q,M", Year=X
  - FixedAssets              : enumerate TableName, Year=X
  - ITA                      : enumerate AreaOrCountry; Indicator/Frequency/Year=ALL
                               (ALL x ALL is rejected: exactly one of Indicator
                               or one country must be fixed; iterating ~131
                               countries is far cheaper than ~889 indicators)
  - IIP                      : enumerate Year; TypeOfInvestment/Component/Freq=ALL
                               (ALL x ALL rejected; ~50 years << ~399 inv. types)
  - IntlServTrade / IntlServSTA : single ALL-everything call
  - GDPbyIndustry / UnderlyingGDPbyIndustry : per Frequency in (A,Q), rest=ALL
  - InputOutput              : enumerate TableID, Year=ALL
  - MNE                      : per DirectionOfInvestment, Classification=Country
                               (>3 ALL params is rejected, so Country is the one
                               classification reachable with Country+Year=all)
  - Regional                 : headline state-annual tables x their LineCodes,
                               GeoFips=STATE, Year=ALL (LineCode=ALL is rejected,
                               so we iterate codes; county/MSA/quarterly tables
                               are deferred — their per-geo payloads are huge and
                               the SA* state series is the representative core)
"""
import json
import os

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
    SqlNodeSpec,
    get,
    list_raw_files,
    load_state,
    raw_writer,
    save_state,
)

STATE_VERSION = 1
BASE_URL = "https://apps.bea.gov/api/data/"

# Entity union — the authoritative coverage target (12 BEA datasets).
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

NODE_TO_DATASET = {f"bea-{e.lower()}": e for e in ENTITY_IDS}

# Headline state-annual Regional tables (stable BEA ids, chosen 2026-06-14).
# GDP, personal income, and PCE by state — the representative bounded core.
REGIONAL_TABLES = [
    "SAGDP1",   # GDP summary by state
    "SAGDP2",   # GDP by state, all industries
    "SAINC1",   # State personal income summary
    "SAINC4",   # Personal income, population, per-capita income
    "SAINC30",  # Economic profile
    "SAPCE1",   # Personal consumption expenditures by state
]


# --------------------------------------------------------------------------
# HTTP transport: rate-limited + retried. raise_for_status inside the retry.
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
@limits(calls=55, period=60)  # ~55% of documented 100/min — headroom for churn
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _request(params: dict) -> dict:
    resp = get(BASE_URL, params=params, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp.json()


def _api_call(**params):
    """Return (results, error). `error` is the BEAAPI error dict or None.

    Checks both the top-level BEAAPI.Error and the per-method Results.Error,
    since BEA returns 200 OK with a structured error in either slot.
    """
    full = {"UserID": os.environ["BEA_API_KEY"], "ResultFormat": "JSON", **params}
    body = _request(full)
    api = body["BEAAPI"]
    err = api.get("Error")
    if err:
        return None, err
    res = api.get("Results")
    if isinstance(res, list):
        res = res[0] if res else {}
    if isinstance(res, dict) and "Error" in res:
        return None, res["Error"]
    return res, None


def _err_str(err: dict) -> str:
    code = err.get("APIErrorCode") or err.get("number") or "?"
    desc = err.get("APIErrorDescription") or err.get("error") or err
    return f"[{code}] {desc}"


def _getdata(dataset: str, **params) -> list:
    """Run one GetData call; return its Data rows ([] on a BEA-level error).

    A BEA-level error (suppressed cell, 'no data found', an unsupported
    frequency for a table) is logged and treated as empty so one bad slice
    doesn't kill the whole dataset crawl. HTTP/transport failures are NOT
    caught here — they propagate through the retry decorator.
    """
    res, err = _api_call(method="GetData", datasetname=dataset, **params)
    if err:
        print(f"[bea] {dataset} GetData {params} -> API error {_err_str(err)}", flush=True)
        return []
    data = res.get("Data") if isinstance(res, dict) else res
    return data or []


def _values(dataset: str, param: str, **filt) -> list:
    """Enumerate the valid values of a parameter (optionally filtered)."""
    if filt:
        res, err = _api_call(
            method="GetParameterValuesFiltered",
            datasetname=dataset,
            TargetParameter=param,
            **filt,
        )
    else:
        res, err = _api_call(
            method="GetParameterValues",
            datasetname=dataset,
            ParameterName=param,
        )
    if err:
        raise RuntimeError(f"{dataset}.{param} parameter values: {_err_str(err)}")
    vals = res.get("ParamValue", res) if isinstance(res, dict) else res
    if isinstance(vals, dict):
        vals = [vals]
    out = []
    for v in vals:
        for k in ("Key", "key", "TableName", "TableID", "Indicator"):
            if k in v:
                out.append(str(v[k]))
                break
        else:
            out.append(str(next(iter(v.values()))))
    return out


# --------------------------------------------------------------------------
# Per-dataset row producers (generators yielding raw observation dicts).
# --------------------------------------------------------------------------
def _gen_table_family(dataset: str, with_frequency: bool):
    tables = _values(dataset, "TableName")
    for i, t in enumerate(tables):
        params = {"TableName": t, "Year": "X"}
        if with_frequency:
            params["Frequency"] = "A,Q,M"
        yield from _getdata(dataset, **params)
        if i % 25 == 0:
            print(f"[bea] {dataset}: {i + 1}/{len(tables)} tables", flush=True)


def _gen_ita():
    countries = _values("ITA", "AreaOrCountry")
    for i, c in enumerate(countries):
        yield from _getdata(
            "ITA", Indicator="ALL", AreaOrCountry=c, Frequency="ALL", Year="ALL"
        )
        if i % 20 == 0:
            print(f"[bea] ITA: {i + 1}/{len(countries)} areas", flush=True)


def _gen_iip():
    years = _values("IIP", "Year")
    for y in years:
        yield from _getdata(
            "IIP", TypeOfInvestment="ALL", Component="ALL", Frequency="ALL", Year=y
        )


def _gen_intlservtrade():
    yield from _getdata(
        "IntlServTrade",
        TypeOfService="ALL",
        TradeDirection="ALL",
        Affiliation="ALL",
        AreaOrCountry="AllCountries",
        Year="ALL",
    )


def _gen_intlservsta():
    yield from _getdata(
        "IntlServSTA",
        Channel="ALL",
        Destination="ALL",
        Industry="ALL",
        AreaOrCountry="AllCountries",
        Year="ALL",
    )


def _gen_gdpbyindustry(dataset: str):
    # ALL frequency is rejected; iterate the two published frequencies.
    for freq in ("A", "Q"):
        yield from _getdata(
            dataset, Frequency=freq, Industry="ALL", TableID="ALL", Year="ALL"
        )


def _gen_inputoutput():
    tids = _values("InputOutput", "TableID")
    for tid in tids:
        yield from _getdata("InputOutput", TableID=tid, Year="ALL")


def _gen_mne():
    # Country classification is the one reachable with Country+Year=all without
    # tripping the ">3 ALL parameters" limit. DirectionOfInvestment is not echoed
    # in the rows, so inject it for the transform.
    for direction in ("outward", "inward", "parent"):
        for row in _getdata(
            "MNE",
            DirectionOfInvestment=direction,
            Classification="Country",
            Country="all",
            Year="all",
        ):
            row["DirectionOfInvestment"] = direction
            yield row


def _gen_regional():
    for t in REGIONAL_TABLES:
        linecodes = _values("Regional", "LineCode", TableName=t)
        for lc in linecodes:
            for row in _getdata(
                "Regional", TableName=t, GeoFips="STATE", LineCode=lc, Year="ALL"
            ):
                # TableName/LineCode aren't echoed per row — inject for the transform.
                row["TableName"] = t
                row["LineCode"] = lc
                yield row
        print(f"[bea] Regional: finished {t} ({len(linecodes)} line codes)", flush=True)


def _produce(node_id: str):
    ds = NODE_TO_DATASET[node_id]
    if ds in ("NIPA", "NIUnderlyingDetail"):
        yield from _gen_table_family(ds, with_frequency=True)
    elif ds == "FixedAssets":
        yield from _gen_table_family(ds, with_frequency=False)
    elif ds == "ITA":
        yield from _gen_ita()
    elif ds == "IIP":
        yield from _gen_iip()
    elif ds == "IntlServTrade":
        yield from _gen_intlservtrade()
    elif ds == "IntlServSTA":
        yield from _gen_intlservsta()
    elif ds in ("GDPbyIndustry", "UnderlyingGDPbyIndustry"):
        yield from _gen_gdpbyindustry(ds)
    elif ds == "InputOutput":
        yield from _gen_inputoutput()
    elif ds == "MNE":
        yield from _gen_mne()
    elif ds == "Regional":
        yield from _gen_regional()
    else:  # pragma: no cover — NODE_TO_DATASET is the closed set of entities
        raise KeyError(f"no producer for dataset {ds!r}")


def fetch_one(node_id: str) -> None:
    """Stream one BEA dataset's full observation set to a gzip ndjson raw asset.

    Rows are written as they arrive (never fully held in memory) — ITA/IIP/IO
    each run into the millions of rows. Raw is written before state, always.
    """
    asset = node_id
    n = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as f:
        for row in _produce(node_id):
            f.write(json.dumps(row, ensure_ascii=True))
            f.write("\n")
            n += 1
    print(f"[bea] {asset}: wrote {n} rows", flush=True)
    if n == 0:
        # A dataset that yields nothing means the catalog walk or GetData
        # contract changed — fail loudly rather than publishing an empty table.
        raise RuntimeError(f"{asset}: produced 0 rows")

    save_state(asset, {"schema_version": STATE_VERSION, "last_run_stats": {"records": n}})


DOWNLOAD_SPECS = [
    NodeSpec(id=f"bea-{eid.lower()}", fn=fetch_one, kind="download")
    for eid in ENTITY_IDS
]


# --------------------------------------------------------------------------
# Transforms — one published Delta table per dataset. Thin parse-and-type pass.
# --------------------------------------------------------------------------
# DataValue arrives as a string with thousands separators and suppression
# markers ((D)/(NA)/blank); coerce to DOUBLE and drop the unparseable.
_VAL = "TRY_CAST(NULLIF(REPLACE(CAST(DataValue AS VARCHAR), ',', ''), '') AS DOUBLE)"
_MNE_VAL = "TRY_CAST(NULLIF(REPLACE(CAST(DataValueUnformatted AS VARCHAR), ',', ''), '') AS DOUBLE)"


def _table_family_sql(dep: str) -> str:
    return f'''
        SELECT
            CAST(TableName AS VARCHAR)        AS table_name,
            CAST(SeriesCode AS VARCHAR)       AS series_code,
            TRY_CAST(LineNumber AS INTEGER)   AS line_number,
            CAST(LineDescription AS VARCHAR)  AS line_description,
            CAST(TimePeriod AS VARCHAR)       AS time_period,
            CAST(METRIC_NAME AS VARCHAR)      AS metric_name,
            CAST(CL_UNIT AS VARCHAR)          AS unit,
            TRY_CAST(UNIT_MULT AS INTEGER)    AS unit_mult,
            {_VAL}                            AS value,
            CAST(NoteRef AS VARCHAR)          AS note_ref
        FROM "{dep}"
        WHERE {_VAL} IS NOT NULL
    '''


def _ts_family_sql(dep: str, dims: list) -> str:
    dim_cols = ",\n            ".join(
        f"CAST({src} AS VARCHAR) AS {alias}" for src, alias in dims
    )
    return f'''
        SELECT
            {dim_cols},
            TRY_CAST(Year AS INTEGER)                  AS year,
            CAST(TimePeriod AS VARCHAR)                AS time_period,
            CAST(TimeSeriesId AS VARCHAR)              AS time_series_id,
            CAST(TimeSeriesDescription AS VARCHAR)     AS time_series_description,
            CAST(CL_UNIT AS VARCHAR)                   AS unit,
            TRY_CAST(UNIT_MULT AS INTEGER)             AS unit_mult,
            {_VAL}                                     AS value,
            CAST(NoteRef AS VARCHAR)                   AS note_ref
        FROM "{dep}"
        WHERE {_VAL} IS NOT NULL
    '''


def _gdpbyindustry_sql(dep: str) -> str:
    return f'''
        SELECT
            CAST(TableID AS VARCHAR)              AS table_id,
            CAST(Frequency AS VARCHAR)            AS frequency,
            TRY_CAST(Year AS INTEGER)             AS year,
            CAST(Quarter AS VARCHAR)              AS quarter,
            CAST(Industry AS VARCHAR)             AS industry,
            CAST(IndustrYDescription AS VARCHAR)  AS industry_description,
            {_VAL}                                AS value,
            CAST(NoteRef AS VARCHAR)              AS note_ref
        FROM "{dep}"
        WHERE {_VAL} IS NOT NULL
    '''


def _inputoutput_sql(dep: str) -> str:
    return f'''
        SELECT
            CAST(TableID AS VARCHAR)   AS table_id,
            TRY_CAST(Year AS INTEGER)  AS year,
            CAST(RowCode AS VARCHAR)   AS row_code,
            CAST(RowDescr AS VARCHAR)  AS row_descr,
            CAST(RowType AS VARCHAR)   AS row_type,
            CAST(ColCode AS VARCHAR)   AS col_code,
            CAST(ColDescr AS VARCHAR)  AS col_descr,
            CAST(ColType AS VARCHAR)   AS col_type,
            {_VAL}                     AS value,
            CAST(NoteRef AS VARCHAR)   AS note_ref
        FROM "{dep}"
        WHERE {_VAL} IS NOT NULL
    '''


def _mne_sql(dep: str) -> str:
    return f'''
        SELECT
            CAST(DirectionOfInvestment AS VARCHAR)  AS direction_of_investment,
            TRY_CAST(Year AS INTEGER)               AS year,
            CAST(SeriesID AS VARCHAR)               AS series_id,
            CAST(SeriesName AS VARCHAR)             AS series_name,
            CAST(Row AS VARCHAR)                    AS row_label,
            CAST(RowCode AS VARCHAR)                AS row_code,
            CAST(Column AS VARCHAR)                 AS column_label,
            CAST(ColumnCode AS VARCHAR)             AS column_code,
            CAST(TableScale AS VARCHAR)             AS table_scale,
            {_MNE_VAL}                              AS value,
            CAST(DataValue AS VARCHAR)              AS value_formatted
        FROM "{dep}"
        WHERE {_MNE_VAL} IS NOT NULL
    '''


def _regional_sql(dep: str) -> str:
    return f'''
        SELECT
            CAST(TableName AS VARCHAR)       AS table_name,
            CAST(LineCode AS VARCHAR)        AS line_code,
            CAST(Code AS VARCHAR)            AS series_code,
            CAST(GeoFips AS VARCHAR)         AS geo_fips,
            CAST(GeoName AS VARCHAR)         AS geo_name,
            CAST(TimePeriod AS VARCHAR)      AS time_period,
            CAST(CL_UNIT AS VARCHAR)         AS unit,
            TRY_CAST(UNIT_MULT AS INTEGER)   AS unit_mult,
            {_VAL}                           AS value,
            CAST(NoteRef AS VARCHAR)         AS note_ref
        FROM "{dep}"
        WHERE {_VAL} IS NOT NULL
    '''


def _sql_for(node_id: str) -> str:
    ds = NODE_TO_DATASET[node_id]
    if ds in ("NIPA", "NIUnderlyingDetail", "FixedAssets"):
        return _table_family_sql(node_id)
    if ds == "ITA":
        return _ts_family_sql(node_id, [("Indicator", "indicator"), ("AreaOrCountry", "area_or_country"), ("Frequency", "frequency")])
    if ds == "IIP":
        return _ts_family_sql(node_id, [("TypeOfInvestment", "type_of_investment"), ("Component", "component"), ("Frequency", "frequency")])
    if ds == "IntlServTrade":
        return _ts_family_sql(node_id, [("TypeOfService", "type_of_service"), ("TradeDirection", "trade_direction"), ("Affiliation", "affiliation"), ("AreaOrCountry", "area_or_country")])
    if ds == "IntlServSTA":
        return _ts_family_sql(node_id, [("Channel", "channel"), ("Destination", "destination"), ("Industry", "industry"), ("AreaOrCountry", "area_or_country")])
    if ds in ("GDPbyIndustry", "UnderlyingGDPbyIndustry"):
        return _gdpbyindustry_sql(node_id)
    if ds == "InputOutput":
        return _inputoutput_sql(node_id)
    if ds == "MNE":
        return _mne_sql(node_id)
    if ds == "Regional":
        return _regional_sql(node_id)
    raise KeyError(f"no transform SQL for dataset {ds!r}")  # pragma: no cover


TRANSFORM_SPECS = [
    SqlNodeSpec(id=f"{s.id}-transform", deps=[s.id], sql=_sql_for(s.id))
    for s in DOWNLOAD_SPECS
]
