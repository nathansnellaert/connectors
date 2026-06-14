"""BJS connector — Bureau of Justice Statistics aggregate datasets.

Source: the Socrata-style REST API at https://api.ojp.gov/bjsdataset/v1/<resource_id>.json
covering 10 published BJS datasets in two groups:
  - NCVS Select (4 resources): personal/household incident-level survey records
    (victimization) plus the population denominator tables. These are large —
    the personal-population table (r4j4-fdwx) runs to ~6.3M rows.
  - NIBRS National Estimates (6 resources): aggregated estimate tables keyed on
    indicator_name (incidents/offenses/victimization counts, rates).

Fetch shape: stateless full re-pull. There is no incremental/since filter on
this API and the datasets are revised annually, so we refetch the whole corpus
each run and overwrite.

Raw format: parquet with an EXPLICIT per-resource all-string schema.
Why not NDJSON: the NIBRS tables exhibit JSON key drift — later rows carry extra
keys (e.g. `estimate_copula`) absent from the header rows. DuckDB's read_json_auto
samples a prefix and then errors ("unknown key ...") on the first drift row, which
killed the prior attempt. Building a parquet with a fixed schema picks only the
documented columns (extra keys are dropped, missing keys become null) and the
transforms then read a stable read_parquet view.

Pagination: the API silently caps a query at 1000 rows unless $limit is given,
and several tables far exceed any single $limit (~6.3M rows). So we page with
$limit + $offset ordered by :id until a short/empty page rather than trusting one
big $limit (which would silently truncate). Streamed to parquet row groups so the
multi-million-row tables never sit fully in memory.
"""
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
    raw_parquet_writer,
    save_state,
)

BASE = "https://api.ojp.gov/bjsdataset/v1/"
PAGE_SIZE = 50000
# Safety ceiling: 1000 pages * 50k = 50M rows. The biggest table today is ~6.3M
# rows (127 pages). Exhausting this means the source grew far past expectations —
# raise rather than silently truncate.
MAX_PAGES = 1000
STATE_VERSION = 1

# Documented (stable, header-row) column set per resource. Probed live 2026-06-14
# from the first rows of each resource. Drift keys observed in later NIBRS rows
# are intentionally excluded — pyarrow drops dict keys absent from the schema.
COLUMNS = {
    # NCVS — personal victimization (incident-level)
    "gcuy-rt5g": [
        "idper", "yearq", "year", "ager", "sex", "hispanic", "race",
        "race_ethnicity", "hincome1", "hincome2", "marital", "popsize", "region",
        "msa", "locality", "educatn1", "educatn2", "veteran", "citizen",
        "newcrime", "newoff", "serious", "seriousviolent", "notify", "vicservices",
        "locationr", "direl", "weapon", "weapcat", "injury", "offenderage",
        "offendersex", "offtracenew", "treatment", "series", "wgtviccy", "newwgt",
    ],
    # NCVS — household victimization
    "gkck-euys": [
        "idhh", "yearq", "year", "hhage", "hhsex", "hhhisp", "hhrace",
        "hhrace_ethnicity", "hincome1", "hincome2", "hnumber", "popsize", "region",
        "msa", "locality", "newcrime", "newoff", "notify", "vicservices",
        "locationr", "series", "wgtviccy", "newwgt",
    ],
    # NCVS — personal population (denominators)
    "r4j4-fdwx": [
        "idper", "yearq", "year", "ager", "sex", "hispanic", "race",
        "race_ethnicity", "hincome1", "hincome2", "marital", "popsize", "region",
        "msa", "locality", "educatn1", "educatn2", "veteran", "citizen", "wgtpercy",
    ],
    # NCVS — household population (denominators)
    "ya4e-n9zp": [
        "idhh", "yearq", "year", "hhage", "hhsex", "hhhisp", "hhrace",
        "hhrace_ethnicity", "hincome1", "hincome2", "hnumber", "popsize", "region",
        "msa", "locality", "wgthhcy",
    ],
}
# All 6 NIBRS National Estimates resources share the same 34-column schema.
_NIBRS_COLS = [
    "indicator_name", "estimate", "estimate_unweighted", "estimate_geographic_location",
    "estimate_type", "estimate_type_num", "estimate_domain_1", "estimate_standard_error",
    "estimate_upper_bound", "estimate_lower_bound", "estimate_bias", "estimate_prb",
    "estimate_rmse", "relative_standard_error", "relative_rmse", "prb_actual",
    "analysis_weight_name", "population_estimate", "pop_cov", "agency_counts",
    "permutation_number", "full_table", "estimates_version", "time_series_start_year",
    "suppression_flag_indicator", "der_variable_name", "der_elig_suppression",
    "der_perm_group_suppression", "der_perm_group_unsuppression", "der_rrmse_30",
    "der_rrmse_gt_30_se_estimate", "der_rrmse_gt_30_se_estimate_1",
    "poptotal_orig_elig_perm_agency", "poptotal_orig_univ_elig_perm",
]
NCVS_IDS = ["gcuy-rt5g", "gkck-euys", "r4j4-fdwx", "ya4e-n9zp"]
NIBRS_IDS = ["iv7i-eah6", "kj7p-vx4s", "ms42-n765", "r32q-bdaw", "uy37-xgmh", "x3sz-eb6y"]
for _nid in NIBRS_IDS:
    COLUMNS[_nid] = _NIBRS_COLS
ENTITY_IDS = NCVS_IDS + NIBRS_IDS

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
def _fetch_page(resource_id: str, offset: int) -> list:
    resp = get(
        f"{BASE}{resource_id}.json",
        params={"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"},
        timeout=(10.0, 180.0),
    )
    resp.raise_for_status()
    return resp.json()


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    resource_id = node_id[len("bjs-"):]
    cols = COLUMNS[resource_id]
    schema = pa.schema([(c, pa.string()) for c in cols])

    total = 0
    with raw_parquet_writer(asset, schema) as writer:
        for page in range(MAX_PAGES):
            rows = _fetch_page(resource_id, page * PAGE_SIZE)
            if not rows:
                break
            # Project to the documented columns + stringify (Socrata serializes
            # everything as strings, but coerce defensively). Extra drift keys
            # are dropped; missing keys become null.
            data = {
                c: [(str(r[c]) if r.get(c) is not None else None) for r in rows]
                for c in cols
            }
            writer.write_table(pa.table(data, schema=schema))
            total += len(rows)
            if page % 10 == 0:
                print(f"{asset}: {total} rows fetched (page {page})", flush=True)
            if len(rows) < PAGE_SIZE:
                break
        else:
            raise RuntimeError(
                f"{asset}: hit MAX_PAGES={MAX_PAGES} safety cap "
                f"({total} rows) — source grew past expectations"
            )
    # Raw written and flushed (context exit) before state, always.
    save_state(asset, {"schema_version": STATE_VERSION, "last_run_stats": {"records": total}})
    print(f"{asset}: done, {total} rows", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id=f"bjs-{eid}", fn=fetch_one, kind="download")
    for eid in ENTITY_IDS
]

# Transforms: one published Delta table per resource. Thin parse-and-type pass.
# NCVS tables key on `year` (cast INTEGER, drop year-null junk); NIBRS estimate
# tables key on `indicator_name` + `estimate` (cast DOUBLE, drop indicator-null).
# All other columns pass through as strings.
TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"bjs-{eid}-transform",
        deps=[f"bjs-{eid}"],
        sql=(
            f'SELECT * REPLACE (TRY_CAST(year AS INTEGER) AS year) '
            f'FROM "bjs-{eid}" WHERE TRY_CAST(year AS INTEGER) IS NOT NULL'
        ),
    )
    for eid in NCVS_IDS
] + [
    SqlNodeSpec(
        id=f"bjs-{eid}-transform",
        deps=[f"bjs-{eid}"],
        sql=(
            f'SELECT * REPLACE (TRY_CAST(estimate AS DOUBLE) AS estimate) '
            f'FROM "bjs-{eid}" WHERE indicator_name IS NOT NULL'
        ),
    )
    for eid in NIBRS_IDS
]
