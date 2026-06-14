"""BEA connector — Bureau of Economic Analysis REST API.

One download node per BEA dataset (the 12-entity union). Each node walks the
dataset's catalog (GetParameterValues / GetParameterValuesFiltered) and drives
GetData, streaming observations to a single gzip ndjson raw asset. A SQL
transform per dataset projects/casts that raw into a published Delta table.

Access strategy (per research handoff): REST only, base
https://apps.bea.gov/api/data/. Auth is a free 36-char UserID passed as the
`UserID` query param, read from BEA_API_KEY. Every response is wrapped in
{BEAAPI:{Request, Results|Error}}; Results may itself carry an Error, so error
detection checks the JSON body, never just HTTP status.

Fetch shape: STATELESS FULL RE-PULL of a BOUNDED, representative slice per
dataset. BEA exposes no since/modifiedAfter filter on any method, so every
refresh re-fetches and overwrites; revisions are picked up for free. The
maintain step (authored later) gates whether a node runs on a given refresh.

RATE-LIMIT DESIGN (this is why the slice is bounded) — documented per UserID:
100 req/min, 100 MB/min, 30 errors/min; exceeding ANY of the three throttles
the key for 1 hour. BEA GetData payloads are large (one NIPA table ~2.6 MB; the
GDPbyIndustry "ALL" call ~29 MB), so the *bandwidth* ceiling — not the request
count — is the binding constraint: a full ~300-table NIPA enumeration at even
55 req/min sustains ~140 MB/min and throttles the key. We therefore (a) bound
each dataset to a representative slice (the harness coverage target is the
dataset, not every table within it), and (b) pace by BYTES, sleeping after each
response so the rolling 60 s window stays under ~50 MB/min. Bounds are logged so
truncation is never silent. 429 is treated as a transient and retried with
backoff; if it persists (the 1 h throttle) the node fails loudly — a hard stop.

Per-dataset GetData driving (parameter surfaces verified live 2026-06-14):
  - NIPA / NIUnderlyingDetail : first N TableName, Frequency="A,Q", Year="ALL"
                               (one call returns both A and Q; M dropped — most
                               tables lack it and it inflates the error budget)
  - FixedAssets              : first N TableName, Year="ALL" (annual, no Freq)
  - ITA                      : first N AreaOrCountry; Indicator/Frequency=ALL,
                               Year="ALL" (ALL x ALL is rejected — exactly one
                               of Indicator or country must be fixed)
  - IIP                      : most-recent N Year; TypeOfInvestment/Component/
                               Frequency=ALL (ALL x ALL rejected)
  - IntlServTrade / IntlServSTA : single ALL-everything call (bounded already)
  - GDPbyIndustry / UnderlyingGDPbyIndustry : annual only (Q dropped — it ~4x's
                               the 29 MB annual payload), Industry/TableID=ALL
  - InputOutput              : first N TableID, Year="ALL"
  - MNE                      : per DirectionOfInvestment, Classification=Country,
                               Country=all, Year=all (>3 ALL params rejected)
  - Regional                 : headline state-annual tables x first N LineCodes,
                               GeoFips=STATE, Year="ALL" (LineCode=ALL rejected)
"""
import json
import os
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
    SqlNodeSpec,
    get,
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

# --- Scope bounds (deliberate, dated 2026-06-14; logged when they truncate) ---
# These cap each dataset to a representative slice so the whole DAG stays well
# under BEA's 100 MB/min bandwidth ceiling. They are NOT safety caps on source
# growth — they are intentional scope limits; the fetch logs "fetched X of Y".
MAX_NIPA_TABLES = 10
MAX_NIUND_TABLES = 8
MAX_FIXEDASSETS_TABLES = 8
MAX_INPUTOUTPUT_TABLES = 8
MAX_ITA_AREAS = 12
MAX_IIP_YEARS = 12
MAX_REGIONAL_LINECODES = 5

# Headline state-annual Regional tables (stable BEA ids, chosen 2026-06-14):
# GDP, personal income, and PCE by state — the representative bounded core.
REGIONAL_TABLES = [
    "SAGDP1",   # GDP summary by state
    "SAGDP2",   # GDP by state, all industries
    "SAINC1",   # State personal income summary
    "SAINC4",   # Personal income, population, per-capita income
    "SAINC30",  # Economic profile
    "SAPCE1",   # Personal consumption expenditures by state
]

# Bandwidth pacer: sleep after each response so the rolling 60 s window holds
# under ~50 MB/min (half the documented 100 MB/min, for headroom). Per-process
# state — the DAG runs nodes sequentially (DAG_PARALLELISM=1), so one process
# owns the budget; the trailing sleep also buffers the next node's first call.
_MIN_GAP_S = 1.5                       # >=1.5 s gap -> <=40 req/min << 100/min
_BYTES_PER_SEC = 50_000_000 / 60.0     # ~833 KB/s -> ~50 MB/min ceiling
_PACE = {"prev_bytes": 0}


def _pace() -> None:
    nbytes = _PACE["prev_bytes"]
    if not nbytes:
        return
    time.sleep(max(_MIN_GAP_S, nbytes / _BYTES_PER_SEC))


# --------------------------------------------------------------------------
# HTTP transport: paced + retried. raise_for_status inside the retry so 429/5xx
# are classified as transient and backed off; the pacer runs on every attempt.
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


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _request(params: dict) -> dict:
    _pace()
    resp = get(BASE_URL, params=params, timeout=(10.0, 300.0))
    _PACE["prev_bytes"] = len(resp.content)  # count bandwidth before raising
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
# Each is bounded; the bound is logged when it truncates the source list.
# --------------------------------------------------------------------------
def _gen_table_family(dataset: str, with_frequency: bool, max_tables: int):
    tables = _values(dataset, "TableName")
    chosen = tables[:max_tables]
    print(f"[bea] {dataset}: fetching {len(chosen)} of {len(tables)} tables (bounded)", flush=True)
    for t in chosen:
        params = {"TableName": t, "Year": "ALL"}
        if with_frequency:
            params["Frequency"] = "A,Q"  # one call returns both; M dropped
        yield from _getdata(dataset, **params)


def _gen_ita():
    areas = _values("ITA", "AreaOrCountry")
    chosen = areas[:MAX_ITA_AREAS]
    print(f"[bea] ITA: fetching {len(chosen)} of {len(areas)} areas (bounded)", flush=True)
    for c in chosen:
        yield from _getdata(
            "ITA", Indicator="ALL", AreaOrCountry=c, Frequency="ALL", Year="ALL"
        )


def _gen_iip():
    years = _values("IIP", "Year")
    # Most-recent N years (sorted lexically works — all 4-digit strings).
    chosen = sorted(years)[-MAX_IIP_YEARS:]
    print(f"[bea] IIP: fetching {len(chosen)} of {len(years)} years (bounded)", flush=True)
    for y in chosen:
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
    # Annual only: the quarterly payload is ~4x the 29 MB annual one and would
    # dominate the bandwidth budget. Annual ALL is a rich, meaningful table.
    yield from _getdata(
        dataset, Frequency="A", Industry="ALL", TableID="ALL", Year="ALL"
    )


def _gen_inputoutput():
    tids = _values("InputOutput", "TableID")
    chosen = tids[:MAX_INPUTOUTPUT_TABLES]
    print(f"[bea] InputOutput: fetching {len(chosen)} of {len(tids)} tables (bounded)", flush=True)
    for tid in chosen:
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
        chosen = linecodes[:MAX_REGIONAL_LINECODES]
        for lc in chosen:
            for row in _getdata(
                "Regional", TableName=t, GeoFips="STATE", LineCode=lc, Year="ALL"
            ):
                # TableName/LineCode aren't echoed per row — inject for the transform.
                row["TableName"] = t
                row["LineCode"] = lc
                yield row
        print(
            f"[bea] Regional: {t} fetched {len(chosen)} of {len(linecodes)} line codes (bounded)",
            flush=True,
        )


def _produce(node_id: str):
    ds = NODE_TO_DATASET[node_id]
    if ds == "NIPA":
        yield from _gen_table_family(ds, with_frequency=True, max_tables=MAX_NIPA_TABLES)
    elif ds == "NIUnderlyingDetail":
        yield from _gen_table_family(ds, with_frequency=True, max_tables=MAX_NIUND_TABLES)
    elif ds == "FixedAssets":
        yield from _gen_table_family(ds, with_frequency=False, max_tables=MAX_FIXEDASSETS_TABLES)
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
    """Stream one BEA dataset's bounded observation set to a gzip ndjson asset.

    Rows are written as they arrive (never fully held in memory). Raw is written
    before state, always. A dataset that yields zero rows fails loudly — that
    means the catalog walk or GetData contract changed, not an empty source.
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


def _gdpbyindustry_sql(dep: str, with_quarter: bool) -> str:
    # GDPbyIndustry rows carry a Quarter column (annual rows leave it blank);
    # UnderlyingGDPbyIndustry is annual-only and NEVER emits a Quarter key, so
    # referencing it there would raise "column Quarter not found" (verified
    # live 2026-06-14). Emit NULL for the quarter column in that case.
    quarter_col = (
        "CAST(Quarter AS VARCHAR)" if with_quarter else "CAST(NULL AS VARCHAR)"
    )
    return f'''
        SELECT
            CAST(TableID AS VARCHAR)              AS table_id,
            CAST(Frequency AS VARCHAR)            AS frequency,
            TRY_CAST(Year AS INTEGER)             AS year,
            {quarter_col}                         AS quarter,
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
            CAST("Row" AS VARCHAR)                  AS row_label,
            CAST(RowCode AS VARCHAR)                AS row_code,
            CAST("Column" AS VARCHAR)               AS column_label,
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
        return _gdpbyindustry_sql(node_id, with_quarter=(ds == "GDPbyIndustry"))
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
