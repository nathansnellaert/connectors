"""US Census Bureau connector — implement step (downloads + transforms).

Mechanism: REST (api.census.gov/data), the canonical machine-readable surface.
One published Delta table per dataset/vintage in the entity union (1758 total).

Catalog connector — one generic ``fetch_one`` per dataset:
  1. Reconstruct the dataset endpoint from its entity id. The id encodes the
     dataset path + vintage exactly (verified offline against the collect
     catalog's c_dataset/c_vintage for every one of the 1758 entities):
       timeseries-aies-basic -> data/timeseries/aies/basic   (no vintage)
       abscb-2017            -> data/2017/abscb
       acs-acs1-2005         -> data/2005/acs/acs1
  2. Read variables.json + geography.json (both KEY-FREE) to learn the data
     columns and a queryable geography.
  3. Issue ONE data query (get=<vars>&for=<geo>:*) for the broadest geography.
     The data query REQUIRES a free CENSUS_API_KEY (mandatory on every data
     call; all metadata is key-free). The response is a JSON array-of-arrays
     with the column header as row 0.

Bounded by design:
  - Census caps a single query at 50 variables. Datasets carrying more (ACS
    detailed tables expose ~26k) are published with their first 50 data
    variables in one query — a representative table per dataset, not the full
    cube. This matches research's per-entity "one query" strategy and keeps
    the crawl to ~1 request/dataset. Full-cube expansion for specific
    high-value tables is a deliberate later refinement, not done here.
  - We query the BROADEST no-parent geography (smallest geoLevelDisplay,
    usually 'us'), which bounds row counts and avoids parent 'in=' clauses.
    Census serves HTML error pages (Missing/Invalid Key, bad predicate) with
    HTTP 200, so every data body is validated to be a JSON array before use.

Raw format: NDJSON. The 1758 datasets have distinct, undocumented, drifting
column sets and Census returns every value as a string, so a stable per-dataset
parquet schema is neither knowable up front nor safe — NDJSON is the correct
drifty-records writer. The transform is a thin SELECT * passthrough publish gate
(0 rows fails the node).

State: none. Each dataset is a single bounded full re-pull per refresh
(stateless shape 1). Revisions are picked up for free; freshness gating is the
maintain step's job.

SECRET: this connector needs CENSUS_API_KEY declared in connectors.json under
us-census-bureau.secrets and provisioned via sync_secrets. Without it every
data query fails (the prior attempt's sole failure mode).
"""
from __future__ import annotations

import os

import httpx
from tenacity import (
    retry, retry_if_exception, stop_after_attempt, wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, save_raw_ndjson

# Census query cap: at most 50 variables per data call.
MAX_VARS = 50
# Safety ceiling. A broad-geography single query should never return this many
# rows; if it does, the geography assumption is wrong for that dataset — raise
# so growth/mistakes surface instead of silently OOMing the spawn subprocess.
MAX_ROWS = 2_000_000

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
def _get(url: str, params: dict | None = None):
    """GET with retry/backoff. raise_for_status runs inside the retry so 5xx/429
    are retried; the body is inspected by callers (Census returns HTML errors
    with HTTP 200)."""
    resp = get(url, params=params, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp


def _entity_id(node_id: str) -> str:
    return node_id[len("us-census-bureau-"):]


def _base_url(entity_id: str) -> str:
    """Reconstruct the dataset endpoint from its entity id. Verified offline
    against c_dataset/c_vintage for all 1758 entities (0 mismatches)."""
    parts = entity_id.split("-")
    if parts[0] == "timeseries":
        return "https://api.census.gov/data/" + "/".join(parts)
    if parts[-1].isdigit() and len(parts[-1]) == 4:
        return ("https://api.census.gov/data/" + parts[-1]
                + "/" + "/".join(parts[:-1]))
    return "https://api.census.gov/data/" + "/".join(parts)


def _select_variables(var_meta: dict) -> list[str]:
    """Data columns to request: predicate-only geo clauses excluded, capped at
    MAX_VARS. NAME/GEO_ID floated to the front when present so the published
    table carries a human label and a stable geo id."""
    vars_ = var_meta.get("variables", {}) or {}
    skip = {"for", "in", "ucgid"}
    data_vars = [
        k for k, d in vars_.items()
        if k not in skip and not (isinstance(d, dict) and d.get("predicateOnly"))
    ]
    preferred = [v for v in ("NAME", "GEO_ID") if v in data_vars]
    rest = [v for v in data_vars if v not in preferred]
    return (preferred + rest)[:MAX_VARS]


def _select_geo(geo_meta: dict) -> dict | None:
    """Broadest queryable geography: smallest geoLevelDisplay, preferring one
    that needs no parent so a plain for=<geo>:* works. None when the dataset
    exposes no geography (some economic timeseries)."""
    fips = geo_meta.get("fips", []) or []
    if not fips:
        return None
    no_parent = [g for g in fips if not g.get("requires")]
    pool = no_parent or fips
    return sorted(pool, key=lambda g: str(g.get("geoLevelDisplay", "999")))[0]


def _parse_rows(resp) -> list[dict] | None:
    """Census data is a JSON array-of-arrays, header at row 0. Returns a list of
    dicts, or None when the body is not a usable data array (HTML error page,
    empty, or header-only)."""
    text = resp.text.lstrip()
    if not text.startswith("["):
        return None
    data = resp.json()
    if not isinstance(data, list) or len(data) < 2:
        return None
    header, rows = data[0], data[1:]
    if len(rows) > MAX_ROWS:
        raise RuntimeError(
            f"{len(rows)} rows exceeds MAX_ROWS={MAX_ROWS} — broad-geography "
            "assumption wrong for this dataset"
        )
    return [dict(zip(header, r)) for r in rows]


def fetch_one(node_id: str) -> None:
    asset = node_id
    entity_id = _entity_id(node_id)
    base = _base_url(entity_id)

    key = os.environ.get("CENSUS_API_KEY")
    if not key:
        raise RuntimeError(
            "CENSUS_API_KEY env var is required for Census Data API queries "
            "(free, instant signup at api.census.gov/data/key_signup.html). "
            "Declare it in connectors.json -> us-census-bureau.secrets and "
            "provision via sync_secrets."
        )

    # Metadata is key-free.
    variables = _select_variables(_get(base + "/variables.json").json())
    if not variables:
        raise RuntimeError(f"{entity_id}: variables.json exposes no data columns")
    try:
        geo = _select_geo(_get(base + "/geography.json").json())
    except httpx.HTTPStatusError:
        geo = None  # some datasets omit geography.json entirely

    get_clause = ",".join(variables)
    is_timeseries = entity_id.startswith("timeseries-")
    base_params = {"get": get_clause, "key": key}

    # Ordered query variants; commit to the first returning >=1 data row.
    # Census's required-predicate rules vary per dataset and are not fully
    # described in metadata (some timeseries require time=, some datasets have
    # no geography), so we probe a small ordered set rather than guess one shape.
    variants: list[dict] = []
    if geo is not None:
        forclause = f"{geo['name']}:*"
        variants.append({**base_params, "for": forclause})
        if is_timeseries:
            variants.append({**base_params, "for": forclause, "time": "from 2000"})
    if is_timeseries:
        variants.append({**base_params, "time": "from 2000"})
        variants.append({**base_params, "time": "from 1980"})
    variants.append(dict(base_params))

    last_desc = None
    for params in variants:
        # Describe the variant WITHOUT the key — never log the secret.
        last_desc = {k: v for k, v in params.items() if k != "key"}
        resp = _get(base, params=params)
        rows = _parse_rows(resp)
        if rows:
            save_raw_ndjson(rows, asset)
            print(
                f"[census] {entity_id}: {len(rows)} rows x {len(variables)} vars "
                f"via for={params.get('for', '(none)')}",
                flush=True,
            )
            return

    raise RuntimeError(
        f"{entity_id}: no query variant returned data rows "
        f"(base={base}, last variant={last_desc})"
    )


# Entity union — the authoritative coverage target (1758 datasets), copied from
# data/sources/us-census-bureau/steps/.../entity_union.json. One download spec
# per id; each download has exactly one transform consumer below.
ENTITY_IDS = [
    'abscb-2017', 'abscb-2018', 'abscb-2019', 'abscb-2020', 'abscb-2021', 'abscb-2022', 'abscb-2023',
    'abscbo-2017', 'abscbo-2018', 'abscbo-2019', 'abscbo-2020', 'abscbo-2021', 'abscbo-2022',
    'abscbo-2023', 'abscs-2017', 'abscs-2018', 'abscs-2019', 'abscs-2020', 'abscs-2021', 'abscs-2022',
    'abscs-2023', 'absmcb-2020', 'absmcb-2021', 'absmcb-2022', 'absmcb-2023', 'absnesd-2018',
    'absnesd-2019', 'absnesd-2020', 'absnesd-2021', 'absnesd-2022', 'absnesd-2023', 'absnesdo-2018',
    'absnesdo-2019', 'absnesdo-2020', 'absnesdo-2021', 'absnesdo-2022', 'absnesdo-2023', 'abstcb-2018',
    'acs-acs1-2005', 'acs-acs1-2006', 'acs-acs1-2007', 'acs-acs1-2008', 'acs-acs1-2009',
    'acs-acs1-2010', 'acs-acs1-2011', 'acs-acs1-2012', 'acs-acs1-2013', 'acs-acs1-2014',
    'acs-acs1-2015', 'acs-acs1-2016', 'acs-acs1-2017', 'acs-acs1-2018', 'acs-acs1-2019',
    'acs-acs1-2021', 'acs-acs1-2022', 'acs-acs1-2023', 'acs-acs1-2024', 'acs-acs1-cprofile-2010',
    'acs-acs1-cprofile-2011', 'acs-acs1-cprofile-2012', 'acs-acs1-cprofile-2013',
    'acs-acs1-cprofile-2014', 'acs-acs1-cprofile-2015', 'acs-acs1-cprofile-2016',
    'acs-acs1-cprofile-2017', 'acs-acs1-cprofile-2018', 'acs-acs1-cprofile-2019',
    'acs-acs1-cprofile-2021', 'acs-acs1-cprofile-2022', 'acs-acs1-cprofile-2023',
    'acs-acs1-cprofile-2024', 'acs-acs1-profile-2005', 'acs-acs1-profile-2006', 'acs-acs1-profile-2007',
    'acs-acs1-profile-2008', 'acs-acs1-profile-2009', 'acs-acs1-profile-2010', 'acs-acs1-profile-2011',
    'acs-acs1-profile-2012', 'acs-acs1-profile-2013', 'acs-acs1-profile-2014', 'acs-acs1-profile-2015',
    'acs-acs1-profile-2016', 'acs-acs1-profile-2017', 'acs-acs1-profile-2018', 'acs-acs1-profile-2019',
    'acs-acs1-profile-2021', 'acs-acs1-profile-2022', 'acs-acs1-profile-2023', 'acs-acs1-profile-2024',
    'acs-acs1-pums-2004', 'acs-acs1-pums-2005', 'acs-acs1-pums-2006', 'acs-acs1-pums-2007',
    'acs-acs1-pums-2008', 'acs-acs1-pums-2009', 'acs-acs1-pums-2010', 'acs-acs1-pums-2011',
    'acs-acs1-pums-2012', 'acs-acs1-pums-2013', 'acs-acs1-pums-2014', 'acs-acs1-pums-2015',
    'acs-acs1-pums-2016', 'acs-acs1-pums-2017', 'acs-acs1-pums-2018', 'acs-acs1-pums-2019',
    'acs-acs1-pums-2021', 'acs-acs1-pums-2022', 'acs-acs1-pums-2023', 'acs-acs1-pums-2024',
    'acs-acs1-pumspr-2005', 'acs-acs1-pumspr-2006', 'acs-acs1-pumspr-2007', 'acs-acs1-pumspr-2008',
    'acs-acs1-pumspr-2009', 'acs-acs1-pumspr-2010', 'acs-acs1-pumspr-2011', 'acs-acs1-pumspr-2012',
    'acs-acs1-pumspr-2013', 'acs-acs1-pumspr-2014', 'acs-acs1-pumspr-2015', 'acs-acs1-pumspr-2016',
    'acs-acs1-pumspr-2017', 'acs-acs1-pumspr-2018', 'acs-acs1-pumspr-2019', 'acs-acs1-pumspr-2021',
    'acs-acs1-pumspr-2022', 'acs-acs1-pumspr-2023', 'acs-acs1-pumspr-2024',
    'acs-acs1-sdataprofile-cd119-2023', 'acs-acs1-spp-2008', 'acs-acs1-spp-2009', 'acs-acs1-spp-2010',
    'acs-acs1-spp-2011', 'acs-acs1-spp-2012', 'acs-acs1-spp-2013', 'acs-acs1-spp-2014',
    'acs-acs1-spp-2015', 'acs-acs1-spp-2016', 'acs-acs1-spp-2017', 'acs-acs1-spp-2018',
    'acs-acs1-spp-2019', 'acs-acs1-spp-2021', 'acs-acs1-spp-2022', 'acs-acs1-spp-2023',
    'acs-acs1-spp-2024', 'acs-acs1-subject-2010', 'acs-acs1-subject-2011', 'acs-acs1-subject-2012',
    'acs-acs1-subject-2013', 'acs-acs1-subject-2014', 'acs-acs1-subject-2015', 'acs-acs1-subject-2016',
    'acs-acs1-subject-2017', 'acs-acs1-subject-2018', 'acs-acs1-subject-2019', 'acs-acs1-subject-2021',
    'acs-acs1-subject-2022', 'acs-acs1-subject-2023', 'acs-acs1-subject-2024', 'acs-acs3-2007',
    'acs-acs3-2008', 'acs-acs3-2009', 'acs-acs3-2011', 'acs-acs3-2012', 'acs-acs3-2013',
    'acs-acs3-cprofile-2012', 'acs-acs3-cprofile-2013', 'acs-acs3-profile-2007',
    'acs-acs3-profile-2008', 'acs-acs3-profile-2009', 'acs-acs3-profile-2010', 'acs-acs3-profile-2011',
    'acs-acs3-profile-2012', 'acs-acs3-profile-2013', 'acs-acs3-spp-2009', 'acs-acs3-spp-2010',
    'acs-acs3-spp-2011', 'acs-acs3-spp-2012', 'acs-acs3-spp-2013', 'acs-acs3-subject-2010',
    'acs-acs3-subject-2011', 'acs-acs3-subject-2012', 'acs-acs3-subject-2013', 'acs-acs5-2009',
    'acs-acs5-2010', 'acs-acs5-2011', 'acs-acs5-2012', 'acs-acs5-2013', 'acs-acs5-2014',
    'acs-acs5-2015', 'acs-acs5-2016', 'acs-acs5-2017', 'acs-acs5-2018', 'acs-acs5-2019',
    'acs-acs5-2020', 'acs-acs5-2021', 'acs-acs5-2022', 'acs-acs5-2023', 'acs-acs5-2024',
    'acs-acs5-aian-2010', 'acs-acs5-aian-2015', 'acs-acs5-aian-2021', 'acs-acs5-aianprofile-2010',
    'acs-acs5-aianprofile-2015', 'acs-acs5-aianprofile-2021', 'acs-acs5-cprofile-2015',
    'acs-acs5-cprofile-2016', 'acs-acs5-cprofile-2017', 'acs-acs5-cprofile-2018',
    'acs-acs5-cprofile-2019', 'acs-acs5-cprofile-2020', 'acs-acs5-cprofile-2021',
    'acs-acs5-cprofile-2022', 'acs-acs5-cprofile-2023', 'acs-acs5-cprofile-2024', 'acs-acs5-eeo-2018',
    'acs-acs5-profile-2009', 'acs-acs5-profile-2010', 'acs-acs5-profile-2011', 'acs-acs5-profile-2012',
    'acs-acs5-profile-2013', 'acs-acs5-profile-2014', 'acs-acs5-profile-2015', 'acs-acs5-profile-2016',
    'acs-acs5-profile-2017', 'acs-acs5-profile-2018', 'acs-acs5-profile-2019', 'acs-acs5-profile-2020',
    'acs-acs5-profile-2021', 'acs-acs5-profile-2022', 'acs-acs5-profile-2023', 'acs-acs5-profile-2024',
    'acs-acs5-pums-2009', 'acs-acs5-pums-2010', 'acs-acs5-pums-2011', 'acs-acs5-pums-2012',
    'acs-acs5-pums-2013', 'acs-acs5-pums-2014', 'acs-acs5-pums-2015', 'acs-acs5-pums-2016',
    'acs-acs5-pums-2017', 'acs-acs5-pums-2018', 'acs-acs5-pums-2019', 'acs-acs5-pums-2020',
    'acs-acs5-pums-2021', 'acs-acs5-pums-2022', 'acs-acs5-pums-2023', 'acs-acs5-pums-2024',
    'acs-acs5-pumspr-2009', 'acs-acs5-pumspr-2010', 'acs-acs5-pumspr-2011', 'acs-acs5-pumspr-2012',
    'acs-acs5-pumspr-2013', 'acs-acs5-pumspr-2014', 'acs-acs5-pumspr-2015', 'acs-acs5-pumspr-2016',
    'acs-acs5-pumspr-2017', 'acs-acs5-pumspr-2018', 'acs-acs5-pumspr-2019', 'acs-acs5-pumspr-2020',
    'acs-acs5-pumspr-2021', 'acs-acs5-pumspr-2022', 'acs-acs5-pumspr-2023', 'acs-acs5-pumspr-2024',
    'acs-acs5-spt-2010', 'acs-acs5-spt-2015', 'acs-acs5-spt-2021', 'acs-acs5-sptprofile-2010',
    'acs-acs5-sptprofile-2021', 'acs-acs5-subject-2010', 'acs-acs5-subject-2011',
    'acs-acs5-subject-2012', 'acs-acs5-subject-2013', 'acs-acs5-subject-2014', 'acs-acs5-subject-2015',
    'acs-acs5-subject-2016', 'acs-acs5-subject-2017', 'acs-acs5-subject-2018', 'acs-acs5-subject-2019',
    'acs-acs5-subject-2020', 'acs-acs5-subject-2021', 'acs-acs5-subject-2022', 'acs-acs5-subject-2023',
    'acs-acs5-subject-2024', 'acs-acsse-2014', 'acs-acsse-2015', 'acs-acsse-2016', 'acs-acsse-2017',
    'acs-acsse-2018', 'acs-acsse-2019', 'acs-acsse-2021', 'acs-acsse-2022', 'acs-acsse-2023',
    'acs-acsse-2024', 'acs-flows-2010', 'acs-flows-2011', 'acs-flows-2012', 'acs-flows-2013',
    'acs-flows-2014', 'acs-flows-2015', 'acs-flows-2016', 'acs-flows-2017', 'acs-flows-2018',
    'acs-flows-2019', 'acs-flows-2020', 'acs-flows-2021', 'acs-flows-2022', 'acs1-cd113-2011',
    'acs1-cd115-2015', 'acs5-2009', 'aiesnonemp-2023', 'cbp-1986', 'cbp-1987', 'cbp-1988', 'cbp-1989',
    'cbp-1990', 'cbp-1991', 'cbp-1992', 'cbp-1993', 'cbp-1994', 'cbp-1995', 'cbp-1996', 'cbp-1997',
    'cbp-1998', 'cbp-1999', 'cbp-2000', 'cbp-2001', 'cbp-2002', 'cbp-2003', 'cbp-2004', 'cbp-2005',
    'cbp-2006', 'cbp-2007', 'cbp-2008', 'cbp-2009', 'cbp-2010', 'cbp-2011', 'cbp-2012', 'cbp-2013',
    'cbp-2014', 'cbp-2015', 'cbp-2016', 'cbp-2017', 'cbp-2018', 'cbp-2019', 'cbp-2020', 'cbp-2021',
    'cbp-2022', 'cbp-2023', 'cfsarea-2012', 'cfsarea-2017', 'cfsarea-2022', 'cfsexport-2012',
    'cfsexport-2017', 'cfsexport-2022', 'cfshazmat-2012', 'cfshazmat-2017', 'cfshazmat-2022',
    'cfspum-cfspumdest-2012', 'cfspum-cfspumdest-2017', 'cfspum-cfspumorig-2012',
    'cfspum-cfspumorig-2017', 'cfstemp-2017', 'cfstemp-2022', 'cps-arts-feb-2013', 'cps-arts-feb-2014',
    'cps-arts-feb-2015', 'cps-arts-feb-2016', 'cps-arts-feb-2018', 'cps-arts-feb-2020',
    'cps-asec-mar-1992', 'cps-asec-mar-1993', 'cps-asec-mar-1994', 'cps-asec-mar-1995',
    'cps-asec-mar-1996', 'cps-asec-mar-1997', 'cps-asec-mar-1998', 'cps-asec-mar-1999',
    'cps-asec-mar-2000', 'cps-asec-mar-2001', 'cps-asec-mar-2002', 'cps-asec-mar-2003',
    'cps-asec-mar-2004', 'cps-asec-mar-2005', 'cps-asec-mar-2006', 'cps-asec-mar-2007',
    'cps-asec-mar-2008', 'cps-asec-mar-2009', 'cps-asec-mar-2010', 'cps-asec-mar-2011',
    'cps-asec-mar-2012', 'cps-asec-mar-2013', 'cps-asec-mar-2014', 'cps-asec-mar-2015',
    'cps-asec-mar-2016', 'cps-asec-mar-2017', 'cps-asec-mar-2018', 'cps-asec-mar-2019',
    'cps-asec-mar-2020', 'cps-asec-mar-2021', 'cps-asec-mar-2022', 'cps-asec-mar-2023',
    'cps-asec-mar-2024', 'cps-asec-mar-2025', 'cps-basic-apr-1989', 'cps-basic-apr-1990',
    'cps-basic-apr-1991', 'cps-basic-apr-1992', 'cps-basic-apr-1993', 'cps-basic-apr-1994',
    'cps-basic-apr-1995', 'cps-basic-apr-1996', 'cps-basic-apr-1997', 'cps-basic-apr-1998',
    'cps-basic-apr-1999', 'cps-basic-apr-2000', 'cps-basic-apr-2001', 'cps-basic-apr-2002',
    'cps-basic-apr-2003', 'cps-basic-apr-2004', 'cps-basic-apr-2005', 'cps-basic-apr-2006',
    'cps-basic-apr-2007', 'cps-basic-apr-2008', 'cps-basic-apr-2009', 'cps-basic-apr-2010',
    'cps-basic-apr-2011', 'cps-basic-apr-2012', 'cps-basic-apr-2013', 'cps-basic-apr-2014',
    'cps-basic-apr-2015', 'cps-basic-apr-2016', 'cps-basic-apr-2017', 'cps-basic-apr-2018',
    'cps-basic-apr-2019', 'cps-basic-apr-2020', 'cps-basic-apr-2021', 'cps-basic-apr-2022',
    'cps-basic-apr-2023', 'cps-basic-apr-2024', 'cps-basic-apr-2025', 'cps-basic-apr-2026',
    'cps-basic-aug-1989', 'cps-basic-aug-1990', 'cps-basic-aug-1991', 'cps-basic-aug-1992',
    'cps-basic-aug-1993', 'cps-basic-aug-1994', 'cps-basic-aug-1995', 'cps-basic-aug-1996',
    'cps-basic-aug-1997', 'cps-basic-aug-1998', 'cps-basic-aug-1999', 'cps-basic-aug-2000',
    'cps-basic-aug-2001', 'cps-basic-aug-2002', 'cps-basic-aug-2003', 'cps-basic-aug-2004',
    'cps-basic-aug-2005', 'cps-basic-aug-2006', 'cps-basic-aug-2007', 'cps-basic-aug-2008',
    'cps-basic-aug-2009', 'cps-basic-aug-2010', 'cps-basic-aug-2011', 'cps-basic-aug-2012',
    'cps-basic-aug-2013', 'cps-basic-aug-2014', 'cps-basic-aug-2015', 'cps-basic-aug-2016',
    'cps-basic-aug-2017', 'cps-basic-aug-2018', 'cps-basic-aug-2019', 'cps-basic-aug-2020',
    'cps-basic-aug-2021', 'cps-basic-aug-2022', 'cps-basic-aug-2023', 'cps-basic-aug-2024',
    'cps-basic-aug-2025', 'cps-basic-dec-1989', 'cps-basic-dec-1990', 'cps-basic-dec-1991',
    'cps-basic-dec-1992', 'cps-basic-dec-1993', 'cps-basic-dec-1994', 'cps-basic-dec-1995',
    'cps-basic-dec-1996', 'cps-basic-dec-1997', 'cps-basic-dec-1998', 'cps-basic-dec-1999',
    'cps-basic-dec-2000', 'cps-basic-dec-2001', 'cps-basic-dec-2002', 'cps-basic-dec-2003',
    'cps-basic-dec-2004', 'cps-basic-dec-2005', 'cps-basic-dec-2006', 'cps-basic-dec-2007',
    'cps-basic-dec-2008', 'cps-basic-dec-2009', 'cps-basic-dec-2010', 'cps-basic-dec-2011',
    'cps-basic-dec-2012', 'cps-basic-dec-2013', 'cps-basic-dec-2014', 'cps-basic-dec-2015',
    'cps-basic-dec-2016', 'cps-basic-dec-2017', 'cps-basic-dec-2018', 'cps-basic-dec-2019',
    'cps-basic-dec-2020', 'cps-basic-dec-2021', 'cps-basic-dec-2022', 'cps-basic-dec-2023',
    'cps-basic-dec-2024', 'cps-basic-dec-2025', 'cps-basic-feb-1989', 'cps-basic-feb-1990',
    'cps-basic-feb-1991', 'cps-basic-feb-1992', 'cps-basic-feb-1993', 'cps-basic-feb-1994',
    'cps-basic-feb-1995', 'cps-basic-feb-1996', 'cps-basic-feb-1997', 'cps-basic-feb-1998',
    'cps-basic-feb-1999', 'cps-basic-feb-2000', 'cps-basic-feb-2001', 'cps-basic-feb-2002',
    'cps-basic-feb-2003', 'cps-basic-feb-2004', 'cps-basic-feb-2005', 'cps-basic-feb-2006',
    'cps-basic-feb-2007', 'cps-basic-feb-2008', 'cps-basic-feb-2009', 'cps-basic-feb-2010',
    'cps-basic-feb-2011', 'cps-basic-feb-2012', 'cps-basic-feb-2013', 'cps-basic-feb-2014',
    'cps-basic-feb-2015', 'cps-basic-feb-2016', 'cps-basic-feb-2017', 'cps-basic-feb-2018',
    'cps-basic-feb-2019', 'cps-basic-feb-2020', 'cps-basic-feb-2021', 'cps-basic-feb-2022',
    'cps-basic-feb-2023', 'cps-basic-feb-2024', 'cps-basic-feb-2025', 'cps-basic-feb-2026',
    'cps-basic-jan-1989', 'cps-basic-jan-1990', 'cps-basic-jan-1991', 'cps-basic-jan-1992',
    'cps-basic-jan-1993', 'cps-basic-jan-1994', 'cps-basic-jan-1995', 'cps-basic-jan-1996',
    'cps-basic-jan-1997', 'cps-basic-jan-1998', 'cps-basic-jan-1999', 'cps-basic-jan-2000',
    'cps-basic-jan-2001', 'cps-basic-jan-2002', 'cps-basic-jan-2003', 'cps-basic-jan-2004',
    'cps-basic-jan-2005', 'cps-basic-jan-2006', 'cps-basic-jan-2007', 'cps-basic-jan-2008',
    'cps-basic-jan-2009', 'cps-basic-jan-2010', 'cps-basic-jan-2011', 'cps-basic-jan-2012',
    'cps-basic-jan-2013', 'cps-basic-jan-2014', 'cps-basic-jan-2015', 'cps-basic-jan-2016',
    'cps-basic-jan-2017', 'cps-basic-jan-2018', 'cps-basic-jan-2019', 'cps-basic-jan-2020',
    'cps-basic-jan-2021', 'cps-basic-jan-2022', 'cps-basic-jan-2023', 'cps-basic-jan-2024',
    'cps-basic-jan-2025', 'cps-basic-jan-2026', 'cps-basic-jul-1989', 'cps-basic-jul-1990',
    'cps-basic-jul-1991', 'cps-basic-jul-1992', 'cps-basic-jul-1993', 'cps-basic-jul-1994',
    'cps-basic-jul-1995', 'cps-basic-jul-1996', 'cps-basic-jul-1997', 'cps-basic-jul-1998',
    'cps-basic-jul-1999', 'cps-basic-jul-2000', 'cps-basic-jul-2001', 'cps-basic-jul-2002',
    'cps-basic-jul-2003', 'cps-basic-jul-2004', 'cps-basic-jul-2005', 'cps-basic-jul-2006',
    'cps-basic-jul-2007', 'cps-basic-jul-2008', 'cps-basic-jul-2009', 'cps-basic-jul-2010',
    'cps-basic-jul-2011', 'cps-basic-jul-2012', 'cps-basic-jul-2013', 'cps-basic-jul-2014',
    'cps-basic-jul-2015', 'cps-basic-jul-2016', 'cps-basic-jul-2017', 'cps-basic-jul-2018',
    'cps-basic-jul-2019', 'cps-basic-jul-2020', 'cps-basic-jul-2021', 'cps-basic-jul-2022',
    'cps-basic-jul-2023', 'cps-basic-jul-2024', 'cps-basic-jul-2025', 'cps-basic-jun-1989',
    'cps-basic-jun-1990', 'cps-basic-jun-1991', 'cps-basic-jun-1992', 'cps-basic-jun-1993',
    'cps-basic-jun-1994', 'cps-basic-jun-1995', 'cps-basic-jun-1996', 'cps-basic-jun-1997',
    'cps-basic-jun-1998', 'cps-basic-jun-1999', 'cps-basic-jun-2000', 'cps-basic-jun-2001',
    'cps-basic-jun-2002', 'cps-basic-jun-2003', 'cps-basic-jun-2004', 'cps-basic-jun-2005',
    'cps-basic-jun-2006', 'cps-basic-jun-2007', 'cps-basic-jun-2008', 'cps-basic-jun-2009',
    'cps-basic-jun-2010', 'cps-basic-jun-2011', 'cps-basic-jun-2012', 'cps-basic-jun-2013',
    'cps-basic-jun-2014', 'cps-basic-jun-2015', 'cps-basic-jun-2016', 'cps-basic-jun-2017',
    'cps-basic-jun-2018', 'cps-basic-jun-2019', 'cps-basic-jun-2020', 'cps-basic-jun-2021',
    'cps-basic-jun-2022', 'cps-basic-jun-2023', 'cps-basic-jun-2024', 'cps-basic-jun-2025',
    'cps-basic-mar-1989', 'cps-basic-mar-1990', 'cps-basic-mar-1991', 'cps-basic-mar-1992',
    'cps-basic-mar-1993', 'cps-basic-mar-1994', 'cps-basic-mar-1995', 'cps-basic-mar-1996',
    'cps-basic-mar-1997', 'cps-basic-mar-1998', 'cps-basic-mar-1999', 'cps-basic-mar-2000',
    'cps-basic-mar-2001', 'cps-basic-mar-2002', 'cps-basic-mar-2003', 'cps-basic-mar-2004',
    'cps-basic-mar-2005', 'cps-basic-mar-2006', 'cps-basic-mar-2007', 'cps-basic-mar-2008',
    'cps-basic-mar-2009', 'cps-basic-mar-2010', 'cps-basic-mar-2011', 'cps-basic-mar-2012',
    'cps-basic-mar-2013', 'cps-basic-mar-2014', 'cps-basic-mar-2015', 'cps-basic-mar-2016',
    'cps-basic-mar-2017', 'cps-basic-mar-2018', 'cps-basic-mar-2019', 'cps-basic-mar-2020',
    'cps-basic-mar-2021', 'cps-basic-mar-2022', 'cps-basic-mar-2023', 'cps-basic-mar-2024',
    'cps-basic-mar-2025', 'cps-basic-mar-2026', 'cps-basic-may-1989', 'cps-basic-may-1990',
    'cps-basic-may-1991', 'cps-basic-may-1992', 'cps-basic-may-1993', 'cps-basic-may-1994',
    'cps-basic-may-1995', 'cps-basic-may-1996', 'cps-basic-may-1997', 'cps-basic-may-1998',
    'cps-basic-may-1999', 'cps-basic-may-2000', 'cps-basic-may-2001', 'cps-basic-may-2002',
    'cps-basic-may-2003', 'cps-basic-may-2004', 'cps-basic-may-2005', 'cps-basic-may-2006',
    'cps-basic-may-2007', 'cps-basic-may-2008', 'cps-basic-may-2009', 'cps-basic-may-2010',
    'cps-basic-may-2011', 'cps-basic-may-2012', 'cps-basic-may-2013', 'cps-basic-may-2014',
    'cps-basic-may-2015', 'cps-basic-may-2016', 'cps-basic-may-2017', 'cps-basic-may-2018',
    'cps-basic-may-2019', 'cps-basic-may-2020', 'cps-basic-may-2021', 'cps-basic-may-2022',
    'cps-basic-may-2023', 'cps-basic-may-2024', 'cps-basic-may-2025', 'cps-basic-may-2026',
    'cps-basic-nov-1989', 'cps-basic-nov-1990', 'cps-basic-nov-1991', 'cps-basic-nov-1992',
    'cps-basic-nov-1993', 'cps-basic-nov-1994', 'cps-basic-nov-1995', 'cps-basic-nov-1996',
    'cps-basic-nov-1997', 'cps-basic-nov-1998', 'cps-basic-nov-1999', 'cps-basic-nov-2000',
    'cps-basic-nov-2001', 'cps-basic-nov-2002', 'cps-basic-nov-2003', 'cps-basic-nov-2004',
    'cps-basic-nov-2005', 'cps-basic-nov-2006', 'cps-basic-nov-2007', 'cps-basic-nov-2008',
    'cps-basic-nov-2009', 'cps-basic-nov-2010', 'cps-basic-nov-2011', 'cps-basic-nov-2012',
    'cps-basic-nov-2013', 'cps-basic-nov-2014', 'cps-basic-nov-2015', 'cps-basic-nov-2016',
    'cps-basic-nov-2017', 'cps-basic-nov-2018', 'cps-basic-nov-2019', 'cps-basic-nov-2020',
    'cps-basic-nov-2021', 'cps-basic-nov-2022', 'cps-basic-nov-2023', 'cps-basic-nov-2024',
    'cps-basic-nov-2025', 'cps-basic-oct-1989', 'cps-basic-oct-1990', 'cps-basic-oct-1991',
    'cps-basic-oct-1992', 'cps-basic-oct-1993', 'cps-basic-oct-1994', 'cps-basic-oct-1995',
    'cps-basic-oct-1996', 'cps-basic-oct-1997', 'cps-basic-oct-1998', 'cps-basic-oct-1999',
    'cps-basic-oct-2000', 'cps-basic-oct-2001', 'cps-basic-oct-2002', 'cps-basic-oct-2003',
    'cps-basic-oct-2004', 'cps-basic-oct-2005', 'cps-basic-oct-2006', 'cps-basic-oct-2007',
    'cps-basic-oct-2008', 'cps-basic-oct-2009', 'cps-basic-oct-2010', 'cps-basic-oct-2011',
    'cps-basic-oct-2012', 'cps-basic-oct-2013', 'cps-basic-oct-2014', 'cps-basic-oct-2015',
    'cps-basic-oct-2016', 'cps-basic-oct-2017', 'cps-basic-oct-2018', 'cps-basic-oct-2019',
    'cps-basic-oct-2020', 'cps-basic-oct-2021', 'cps-basic-oct-2022', 'cps-basic-oct-2023',
    'cps-basic-oct-2024', 'cps-basic-sep-1989', 'cps-basic-sep-1990', 'cps-basic-sep-1991',
    'cps-basic-sep-1992', 'cps-basic-sep-1993', 'cps-basic-sep-1994', 'cps-basic-sep-1995',
    'cps-basic-sep-1996', 'cps-basic-sep-1997', 'cps-basic-sep-1998', 'cps-basic-sep-1999',
    'cps-basic-sep-2000', 'cps-basic-sep-2001', 'cps-basic-sep-2002', 'cps-basic-sep-2003',
    'cps-basic-sep-2004', 'cps-basic-sep-2005', 'cps-basic-sep-2006', 'cps-basic-sep-2007',
    'cps-basic-sep-2008', 'cps-basic-sep-2009', 'cps-basic-sep-2010', 'cps-basic-sep-2011',
    'cps-basic-sep-2012', 'cps-basic-sep-2013', 'cps-basic-sep-2014', 'cps-basic-sep-2015',
    'cps-basic-sep-2016', 'cps-basic-sep-2017', 'cps-basic-sep-2018', 'cps-basic-sep-2019',
    'cps-basic-sep-2020', 'cps-basic-sep-2021', 'cps-basic-sep-2022', 'cps-basic-sep-2023',
    'cps-basic-sep-2024', 'cps-basic-sep-2025', 'cps-civic-nov-2008', 'cps-civic-nov-2009',
    'cps-civic-nov-2010', 'cps-civic-nov-2011', 'cps-civic-nov-2013', 'cps-contworker-feb-1995',
    'cps-contworker-feb-1997', 'cps-contworker-feb-1999', 'cps-contworker-feb-2001',
    'cps-contworker-feb-2005', 'cps-contworker-may-2017', 'cps-disability-jul-2019',
    'cps-disability-jul-2021', 'cps-disability-may-2012', 'cps-dwjt-feb-1996', 'cps-dwjt-feb-1998',
    'cps-dwjt-feb-2000', 'cps-dwjt-jan-2002', 'cps-dwjt-jan-2004', 'cps-dwjt-jan-2006',
    'cps-dwjt-jan-2008', 'cps-dwjt-jan-2010', 'cps-dwjt-jan-2012', 'cps-dwjt-jan-2014',
    'cps-dwjt-jan-2016', 'cps-dwjt-jan-2018', 'cps-dwjt-jan-2020', 'cps-dwjt-jan-2022',
    'cps-dwjt-jan-2024', 'cps-fertility-jun-1998', 'cps-fertility-jun-2000', 'cps-fertility-jun-2002',
    'cps-fertility-jun-2004', 'cps-fertility-jun-2006', 'cps-fertility-jun-2008',
    'cps-fertility-jun-2010', 'cps-fertility-jun-2012', 'cps-fertility-jun-2014',
    'cps-fertility-jun-2016', 'cps-fertility-jun-2018', 'cps-fertility-jun-2020',
    'cps-fertility-jun-2022', 'cps-fertility-jun-2024', 'cps-foodsec-apr-1995', 'cps-foodsec-apr-1997',
    'cps-foodsec-apr-1999', 'cps-foodsec-apr-2001', 'cps-foodsec-aug-1998', 'cps-foodsec-dec-2001',
    'cps-foodsec-dec-2002', 'cps-foodsec-dec-2003', 'cps-foodsec-dec-2004', 'cps-foodsec-dec-2005',
    'cps-foodsec-dec-2006', 'cps-foodsec-dec-2007', 'cps-foodsec-dec-2008', 'cps-foodsec-dec-2009',
    'cps-foodsec-dec-2010', 'cps-foodsec-dec-2011', 'cps-foodsec-dec-2012', 'cps-foodsec-dec-2013',
    'cps-foodsec-dec-2014', 'cps-foodsec-dec-2015', 'cps-foodsec-dec-2016', 'cps-foodsec-dec-2017',
    'cps-foodsec-dec-2018', 'cps-foodsec-dec-2019', 'cps-foodsec-dec-2020', 'cps-foodsec-dec-2021',
    'cps-foodsec-dec-2022', 'cps-foodsec-dec-2023', 'cps-foodsec-dec-2024', 'cps-foodsec-sep-2000',
    'cps-internet-aug-2000', 'cps-internet-dec-1998', 'cps-internet-jul-2011', 'cps-internet-jul-2013',
    'cps-internet-jul-2015', 'cps-internet-nov-1994', 'cps-internet-nov-2017', 'cps-internet-nov-2019',
    'cps-internet-nov-2021', 'cps-internet-nov-2023', 'cps-internet-oct-1997', 'cps-internet-oct-2003',
    'cps-internet-oct-2007', 'cps-internet-oct-2009', 'cps-internet-oct-2010', 'cps-internet-oct-2012',
    'cps-internet-sep-2001', 'cps-pubarts-aug-2002', 'cps-pubarts-jul-2012', 'cps-pubarts-jul-2017',
    'cps-pubarts-jul-2022', 'cps-school-oct-1994', 'cps-school-oct-1995', 'cps-school-oct-1996',
    'cps-school-oct-1997', 'cps-school-oct-1998', 'cps-school-oct-1999', 'cps-school-oct-2000',
    'cps-school-oct-2001', 'cps-school-oct-2002', 'cps-school-oct-2003', 'cps-school-oct-2004',
    'cps-school-oct-2005', 'cps-school-oct-2006', 'cps-school-oct-2007', 'cps-school-oct-2008',
    'cps-school-oct-2009', 'cps-school-oct-2010', 'cps-school-oct-2011', 'cps-school-oct-2012',
    'cps-school-oct-2013', 'cps-school-oct-2014', 'cps-school-oct-2015', 'cps-school-oct-2016',
    'cps-school-oct-2017', 'cps-school-oct-2018', 'cps-school-oct-2019', 'cps-school-oct-2020',
    'cps-school-oct-2021', 'cps-school-oct-2022', 'cps-school-oct-2023', 'cps-school-oct-2024',
    'cps-tobacco-aug-2006', 'cps-tobacco-aug-2010', 'cps-tobacco-jan-2007', 'cps-tobacco-jan-2011',
    'cps-tobacco-jan-2015', 'cps-tobacco-jan-2019', 'cps-tobacco-jan-2023', 'cps-tobacco-jul-2014',
    'cps-tobacco-jul-2018', 'cps-tobacco-may-2006', 'cps-tobacco-may-2010', 'cps-tobacco-may-2015',
    'cps-tobacco-may-2019', 'cps-tobacco-may-2023', 'cps-tobacco-sep-2022', 'cps-unbank-jan-2009',
    'cps-unbank-jun-2011', 'cps-unbank-jun-2013', 'cps-unbank-jun-2015', 'cps-unbank-jun-2017',
    'cps-unbank-jun-2019', 'cps-unbank-jun-2021', 'cps-unbank-jun-2023', 'cps-vets-aug-1995',
    'cps-vets-aug-2001', 'cps-vets-aug-2003', 'cps-vets-aug-2005', 'cps-vets-aug-2007',
    'cps-vets-aug-2009', 'cps-vets-aug-2011', 'cps-vets-aug-2012', 'cps-vets-aug-2013',
    'cps-vets-aug-2014', 'cps-vets-aug-2015', 'cps-vets-aug-2016', 'cps-vets-aug-2017',
    'cps-vets-aug-2018', 'cps-vets-aug-2019', 'cps-vets-aug-2020', 'cps-vets-aug-2021',
    'cps-vets-aug-2022', 'cps-vets-aug-2023', 'cps-vets-aug-2024', 'cps-vets-aug-2025',
    'cps-vets-jul-2010', 'cps-vets-sep-1997', 'cps-vets-sep-1999', 'cps-volunteer-sep-2002',
    'cps-volunteer-sep-2003', 'cps-volunteer-sep-2004', 'cps-volunteer-sep-2005',
    'cps-volunteer-sep-2006', 'cps-volunteer-sep-2007', 'cps-volunteer-sep-2008',
    'cps-volunteer-sep-2009', 'cps-volunteer-sep-2010', 'cps-volunteer-sep-2011',
    'cps-volunteer-sep-2012', 'cps-volunteer-sep-2013', 'cps-volunteer-sep-2014',
    'cps-volunteer-sep-2015', 'cps-volunteer-sep-2019', 'cps-volunteer-sep-2021',
    'cps-volunteer-sep-2023', 'cps-voting-nov-1994', 'cps-voting-nov-1996', 'cps-voting-nov-1998',
    'cps-voting-nov-2000', 'cps-voting-nov-2002', 'cps-voting-nov-2004', 'cps-voting-nov-2006',
    'cps-voting-nov-2008', 'cps-voting-nov-2010', 'cps-voting-nov-2012', 'cps-voting-nov-2014',
    'cps-voting-nov-2016', 'cps-voting-nov-2018', 'cps-voting-nov-2020', 'cps-voting-nov-2022',
    'cps-voting-nov-2024', 'cps-worksched-may-1997', 'cps-worksched-may-2001', 'cps-worksched-may-2004',
    'cre-2016', 'cre-2017', 'cre-2018', 'cre-2019', 'cre-2020', 'cre-2021', 'cre-2022', 'cre-2023',
    'cre-2024', 'crepuertorico-2019', 'crepuertorico-2021', 'crepuertorico-2022', 'crepuertorico-2023',
    'crepuertorico-2024', 'dec-aian-2000', 'dec-aian-2010', 'dec-aianprofile-2000', 'dec-as-2000',
    'dec-as-2010', 'dec-asyoe-2010', 'dec-cd110h-2000', 'dec-cd110hprofile-2000', 'dec-cd110s-2000',
    'dec-cd110sprofile-2000', 'dec-cd113-2010', 'dec-cd113profile-2010', 'dec-cd115-2010',
    'dec-cd115profile-2010', 'dec-cd116-2010', 'dec-cd118-2020', 'dec-cd119-2020', 'dec-cqr-2000',
    'dec-crosstabas-2020', 'dec-crosstabgu-2020', 'dec-crosstabmp-2020', 'dec-crosstabvi-2020',
    'dec-ddhca-2020', 'dec-ddhcb-2020', 'dec-dhc-2020', 'dec-dhcas-2020', 'dec-dhcgu-2020',
    'dec-dhcmp-2020', 'dec-dhcvi-2020', 'dec-dp-2020', 'dec-dpas-2020', 'dec-dpgu-2020',
    'dec-dpmp-2020', 'dec-dpvi-2020', 'dec-gu-2000', 'dec-gu-2010', 'dec-guyoe-2010', 'dec-mp-2000',
    'dec-mp-2010', 'dec-mpyoe-2010', 'dec-pes-2020', 'dec-pl-2000', 'dec-pl-2010', 'dec-pl-2020',
    'dec-plnat-2010', 'dec-sdhc-2020', 'dec-selfresponserate-2020', 'dec-sf1-2000', 'dec-sf1-2010',
    'dec-sf2-2000', 'dec-sf2-2010', 'dec-sf2profile-2000', 'dec-sf3-2000', 'dec-sf3profile-2000',
    'dec-sf4-2000', 'dec-sf4profile-2000', 'dec-sldh-2000', 'dec-sldhprofile-2000', 'dec-slds-2000',
    'dec-sldsprofile-2000', 'dec-vi-2000', 'dec-vi-2010', 'ecn-islandareas-2012',
    'ecn-islandareas-2017', 'ecn-islandareas-2022', 'ecn-islandareas-comp-2012',
    'ecn-islandareas-comp-2017', 'ecn-islandareas-comp-2022', 'ecn-islandareas-ind-2012',
    'ecn-islandareas-ind-2017', 'ecn-islandareas-ind-2022', 'ecn-islandareas-lines-2012',
    'ecn-islandareas-napcs-2017', 'ecn-islandareas-napcs-2022', 'ecnadbnprop-2017', 'ecnadmben-2012',
    'ecnadmben-2017', 'ecnbasic-2012', 'ecnbasic-2017', 'ecnbasic-2022', 'ecnbranddeal-2012',
    'ecnbranddeal-2017', 'ecnbranddeal-2022', 'ecnbridge1-2012', 'ecnbridge1-2017', 'ecnbridge1-2022',
    'ecnbridge2-2012', 'ecnbridge2-2017', 'ecnbridge2-2022', 'ecnbrordeal-2012', 'ecnbrordeal-2017',
    'ecncashadv-2012', 'ecnccard-2012', 'ecnccard-2017', 'ecnccard-2022', 'ecnclcust-2012',
    'ecnclcust-2017', 'ecnclcust-2022', 'ecnclientorg-2022', 'ecncomm-2012', 'ecncomm-2017',
    'ecncomm-2022', 'ecncomp-2012', 'ecncomp-2017', 'ecncomp-2022', 'ecnconact-2012', 'ecnconact-2017',
    'ecnconact-2022', 'ecnconcess-2012', 'ecncrfin-2012', 'ecncrfin-2017', 'ecncrfin-2022',
    'ecndirprem-2017', 'ecndissmed-2012', 'ecndissmed-2017', 'ecnecomm-2022', 'ecnelmenu-2017',
    'ecnelmenu-2022', 'ecnempfunc-2012', 'ecnempfunc-2017', 'ecnempfunc-2022', 'ecnentsup-2012',
    'ecnentsup-2017', 'ecnentsup-2022', 'ecneoyinv-2012', 'ecneoyinv-2017', 'ecneoyinv-2022',
    'ecneoyinvwh-2012', 'ecneoyinvwh-2017', 'ecnequip-2012', 'ecnexpbyaux-2022', 'ecnexpnrg-2012',
    'ecnexpnrg-2017', 'ecnexpsvc-2012', 'ecnexpsvc-2017', 'ecnflspace-2012', 'ecnflspace-2017',
    'ecnfoodsvc-2012', 'ecnfoodsvc-2017', 'ecnfran-2012', 'ecnfran-2017', 'ecngrant-2012',
    'ecngrant-2017', 'ecngrant-2022', 'ecngrmargprof-2022', 'ecnguest-2012', 'ecnguestsize-2012',
    'ecnhosp-2012', 'ecnhosp-2017', 'ecnhosp-2022', 'ecnhotel-2017', 'ecninstr-2017', 'ecninvval-2012',
    'ecninvval-2017', 'ecninvval-2022', 'ecnipa-2012', 'ecnkob-2012', 'ecnkob-2017', 'ecnkob-2022',
    'ecnlabor-2012', 'ecnlabor-2017', 'ecnlifomfg-2012', 'ecnlifomfg-2017', 'ecnlifomfg-2022',
    'ecnlifomine-2012', 'ecnlifomine-2017', 'ecnlifomine-2022', 'ecnlifoval-2012', 'ecnlines-2012',
    'ecnloan-2012', 'ecnloan-2017', 'ecnloccons-2017', 'ecnloccons-2022', 'ecnlocmfg-2012',
    'ecnlocmfg-2017', 'ecnlocmfg-2022', 'ecnlocmine-2012', 'ecnlocmine-2017', 'ecnlocmine-2022',
    'ecnmargin-2012', 'ecnmargin-2017', 'ecnmatfuel-2012', 'ecnmatfuel-2017', 'ecnmatfuel-2022',
    'ecnmealcost-2012', 'ecnmenutype-2012', 'ecnnapcsind-2017', 'ecnnapcsind-2022', 'ecnnapcsprd-2017',
    'ecnnapcsprd-2022', 'ecnpatient-2012', 'ecnpatient-2017', 'ecnpatient-2022', 'ecnpetrfac-2012',
    'ecnpetrfac-2017', 'ecnpetrprod-2012', 'ecnpetrprod-2017', 'ecnpetrrec-2012', 'ecnpetrrec-2017',
    'ecnpetrstat-2012', 'ecnpetrstat-2017', 'ecnprofit-2012', 'ecnprofit-2017', 'ecnpurelec-2012',
    'ecnpurelec-2017', 'ecnpurelec-2022', 'ecnpurgas-2017', 'ecnpurgas-2022', 'ecnpurmode-2012',
    'ecnpurmode-2017', 'ecnpurmode-2022', 'ecnrdacq-2012', 'ecnrdofc-2012', 'ecnrdofc-2017',
    'ecnrdofc-2022', 'ecnseat-2012', 'ecnsize-2012', 'ecnsize-2017', 'ecnsize-2022', 'ecnsocial-2012',
    'ecnsocial-2017', 'ecntelemeds-2022', 'ecntype-2012', 'ecntype-2017', 'ecntype-2022',
    'ecntypepayer-2017', 'ecntypepayer-2022', 'ecntypop-2012', 'ecntypop-2017', 'ecntypop-2022',
    'ecnvalcon-2012', 'ecnvalcon-2017', 'ecnvalcon-2022', 'ewks-1997', 'ewks-2002', 'ewks-2007',
    'ewks-2012', 'intltrade-imp_exp-2014', 'intltrade-imp_exp-2015', 'intltrade-imp_exp-2016',
    'intltrade-imp_exp-2017', 'intltrade-imp_exp-2018', 'nonemp-1997', 'nonemp-1998', 'nonemp-1999',
    'nonemp-2000', 'nonemp-2001', 'nonemp-2002', 'nonemp-2003', 'nonemp-2004', 'nonemp-2005',
    'nonemp-2006', 'nonemp-2007', 'nonemp-2008', 'nonemp-2009', 'nonemp-2010', 'nonemp-2011',
    'nonemp-2012', 'nonemp-2013', 'nonemp-2014', 'nonemp-2015', 'nonemp-2016', 'nonemp-2017',
    'nonemp-2018', 'nonemp-2019', 'nonemp-2020', 'nonemp-2021', 'nonemp-2022', 'nonemp-2023',
    'pdb-blockgroup-2015', 'pdb-blockgroup-2016', 'pdb-blockgroup-2018', 'pdb-blockgroup-2019',
    'pdb-blockgroup-2020', 'pdb-blockgroup-2021', 'pdb-blockgroup-2022', 'pdb-blockgroup-2023',
    'pdb-blockgroup-2024', 'pdb-statecounty-2020', 'pdb-tract-2015', 'pdb-tract-2016', 'pdb-tract-2018',
    'pdb-tract-2019', 'pdb-tract-2020', 'pdb-tract-2021', 'pdb-tract-2022', 'pdb-tract-2023',
    'pdb-tract-2024', 'pep-agesex-2014', 'pep-agespecial5-2014', 'pep-agespecial6-2014',
    'pep-agespecialpr-2014', 'pep-charage-2015', 'pep-charage-2016', 'pep-charage-2017',
    'pep-charage-2018', 'pep-charage-2019', 'pep-charagegroups-2015', 'pep-charagegroups-2016',
    'pep-charagegroups-2017', 'pep-charagegroups-2018', 'pep-charagegroups-2019', 'pep-charv-2023',
    'pep-cochar5-2013', 'pep-cochar5-2014', 'pep-cochar6-2013', 'pep-cochar6-2014',
    'pep-components-2015', 'pep-components-2016', 'pep-components-2017', 'pep-components-2018',
    'pep-components-2019', 'pep-cty-2013', 'pep-cty-2014', 'pep-housing-2013', 'pep-housing-2014',
    'pep-housing-2015', 'pep-housing-2016', 'pep-housing-2017', 'pep-housing-2018', 'pep-housing-2019',
    'pep-int_charage-2000', 'pep-int_charagegroups-1990', 'pep-int_charagegroups-2000',
    'pep-int_housingunits-2000', 'pep-int_natcivpop-1990', 'pep-int_natmonthly-2000',
    'pep-int_natresafo-1990', 'pep-int_natrespop-1990', 'pep-int_population-2000',
    'pep-monthlynatchar5-2013', 'pep-monthlynatchar5-2014', 'pep-monthlynatchar6-2013',
    'pep-monthlynatchar6-2014', 'pep-natmonthly-2015', 'pep-natmonthly-2016', 'pep-natmonthly-2017',
    'pep-natmonthly-2018', 'pep-natmonthly-2019', 'pep-natmonthly-2021', 'pep-natstprc-2013',
    'pep-natstprc-2014', 'pep-natstprc18-2013', 'pep-natstprc18-2014', 'pep-population-2015',
    'pep-population-2016', 'pep-population-2017', 'pep-population-2018', 'pep-population-2019',
    'pep-population-2021', 'pep-prcagesex-2013', 'pep-prcagesex-2014', 'pep-prm-2013', 'pep-prm-2014',
    'pep-prmagesex-2013', 'pep-prmagesex-2014', 'pep-projagegroups-2014', 'pep-projbirths-2014',
    'pep-projdeaths-2014', 'pep-projnat-2014', 'pep-projnim-2014', 'pep-projpop-2014',
    'pep-stchar5-2013', 'pep-stchar5-2014', 'pep-stchar6-2013', 'pep-stchar6-2014', 'pep-subcty-2013',
    'pep-subcty-2014', 'popproj-agegroups-2017', 'popproj-births-2012', 'popproj-births-2017',
    'popproj-deaths-2012', 'popproj-deaths-2017', 'popproj-nat-2017', 'popproj-nim-2012',
    'popproj-nim-2017', 'popproj-pop-2012', 'popproj-pop-2017', 'rhfs-2015', 'rhfs-2018', 'rhfs-2021',
    'rhfs-2024', 'sipp-2022', 'sipp-2023', 'sipp-2024', 'sipp-benefit-1990panel-1990',
    'sipp-benefit-1991panel-1991', 'sipp-core-1990panel-wave1-1990', 'sipp-core-1990panel-wave2-1990',
    'sipp-core-1990panel-wave3-1990', 'sipp-core-1990panel-wave4-1990',
    'sipp-core-1990panel-wave5-1990', 'sipp-core-1990panel-wave6-1990',
    'sipp-core-1990panel-wave7-1990', 'sipp-core-1990panel-wave8-1990',
    'sipp-core-1991panel-wave1-1991', 'sipp-core-1991panel-wave2-1991',
    'sipp-core-1991panel-wave3-1991', 'sipp-core-1991panel-wave4-1991',
    'sipp-core-1991panel-wave5-1991', 'sipp-core-1991panel-wave6-1991',
    'sipp-core-1991panel-wave7-1991', 'sipp-core-1991panel-wave8-1991',
    'sipp-core-1992panel-wave1-1992', 'sipp-core-1992panel-wave2-1992',
    'sipp-core-1992panel-wave3-1992', 'sipp-core-1992panel-wave4-1992',
    'sipp-core-1992panel-wave5-1992', 'sipp-core-1992panel-wave6-1992',
    'sipp-core-1992panel-wave7-1992', 'sipp-core-1992panel-wave8-1992',
    'sipp-core-1992panel-wave9-1992', 'sipp-core-1993panel-wave1-1993',
    'sipp-core-1993panel-wave2-1993', 'sipp-core-1993panel-wave3-1993',
    'sipp-core-1993panel-wave4-1993', 'sipp-core-1993panel-wave5-1993',
    'sipp-core-1993panel-wave6-1993', 'sipp-core-1993panel-wave7-1993',
    'sipp-core-1993panel-wave8-1993', 'sipp-core-1993panel-wave9-1993',
    'sipp-core-1996panel-wave1-1996', 'sipp-core-1996panel-wave10-1996',
    'sipp-core-1996panel-wave11-1996', 'sipp-core-1996panel-wave12-1996',
    'sipp-core-1996panel-wave2-1996', 'sipp-core-1996panel-wave3-1996',
    'sipp-core-1996panel-wave4-1996', 'sipp-core-1996panel-wave5-1996',
    'sipp-core-1996panel-wave6-1996', 'sipp-core-1996panel-wave7-1996',
    'sipp-core-1996panel-wave8-1996', 'sipp-core-1996panel-wave9-1996',
    'sipp-core-2001panel-wave1-2001', 'sipp-core-2001panel-wave2-2001',
    'sipp-core-2001panel-wave3-2001', 'sipp-core-2001panel-wave4-2001',
    'sipp-core-2001panel-wave5-2001', 'sipp-core-2001panel-wave6-2001',
    'sipp-core-2001panel-wave7-2001', 'sipp-core-2001panel-wave8-2001',
    'sipp-core-2001panel-wave9-2001', 'sipp-core-2004panel-wave1-2004',
    'sipp-core-2004panel-wave10-2004', 'sipp-core-2004panel-wave11-2004',
    'sipp-core-2004panel-wave12-2004', 'sipp-core-2004panel-wave2-2004',
    'sipp-core-2004panel-wave3-2004', 'sipp-core-2004panel-wave4-2004',
    'sipp-core-2004panel-wave5-2004', 'sipp-core-2004panel-wave6-2004',
    'sipp-core-2004panel-wave7-2004', 'sipp-core-2004panel-wave8-2004',
    'sipp-core-2004panel-wave9-2004', 'sipp-core-2008panel-wave1-2008',
    'sipp-core-2008panel-wave10-2008', 'sipp-core-2008panel-wave11-2008',
    'sipp-core-2008panel-wave12-2008', 'sipp-core-2008panel-wave13-2008',
    'sipp-core-2008panel-wave14-2008', 'sipp-core-2008panel-wave15-2008',
    'sipp-core-2008panel-wave16-2008', 'sipp-core-2008panel-wave2-2008',
    'sipp-core-2008panel-wave3-2008', 'sipp-core-2008panel-wave4-2008',
    'sipp-core-2008panel-wave5-2008', 'sipp-core-2008panel-wave6-2008',
    'sipp-core-2008panel-wave7-2008', 'sipp-core-2008panel-wave8-2008',
    'sipp-core-2008panel-wave9-2008', 'sipp-topical-1990panel-wave2-1990',
    'sipp-topical-1990panel-wave3-1990', 'sipp-topical-1990panel-wave4-1990',
    'sipp-topical-1990panel-wave6-1990', 'sipp-topical-1990panel-wave7-1990',
    'sipp-topical-1990panel-wave8-1990', 'sipp-topical-1991panel-wave2-1991',
    'sipp-topical-1991panel-wave3-1991', 'sipp-topical-1991panel-wave4-1991',
    'sipp-topical-1991panel-wave5-1991', 'sipp-topical-1991panel-wave6-1991',
    'sipp-topical-1991panel-wave7-1991', 'sipp-topical-1992panel-wave1-1992',
    'sipp-topical-1992panel-wave2-1992', 'sipp-topical-1992panel-wave3-1992',
    'sipp-topical-1992panel-wave4-1992', 'sipp-topical-1992panel-wave6-1992',
    'sipp-topical-1992panel-wave7-1992', 'sipp-topical-1992panel-wave9-1992',
    'sipp-topical-1993panel-wave1-1993', 'sipp-topical-1993panel-wave2-1993',
    'sipp-topical-1993panel-wave3-1993', 'sipp-topical-1993panel-wave4-1993',
    'sipp-topical-1993panel-wave6-1993', 'sipp-topical-1993panel-wave7-1993',
    'sipp-topical-1993panel-wave9-1993', 'sipp-topical-1996panel-wave1-1996',
    'sipp-topical-1996panel-wave10-1996', 'sipp-topical-1996panel-wave11-1996',
    'sipp-topical-1996panel-wave12-1996', 'sipp-topical-1996panel-wave2-1996',
    'sipp-topical-1996panel-wave3-1996', 'sipp-topical-1996panel-wave4-1996',
    'sipp-topical-1996panel-wave5-1996', 'sipp-topical-1996panel-wave6-1996',
    'sipp-topical-1996panel-wave7-1996', 'sipp-topical-1996panel-wave8-1996',
    'sipp-topical-1996panel-wave9-1996', 'sipp-topical-2001panel-wave1-2001',
    'sipp-topical-2001panel-wave2-2001', 'sipp-topical-2001panel-wave3-2001',
    'sipp-topical-2001panel-wave4-2001', 'sipp-topical-2001panel-wave5-2001',
    'sipp-topical-2001panel-wave6-2001', 'sipp-topical-2001panel-wave7-2001',
    'sipp-topical-2001panel-wave8-2001', 'sipp-topical-2001panel-wave9-2001',
    'sipp-topical-2004panel-wave1-2004', 'sipp-topical-2004panel-wave2-2004',
    'sipp-topical-2004panel-wave3-2004', 'sipp-topical-2004panel-wave4-2004',
    'sipp-topical-2004panel-wave5-2004', 'sipp-topical-2004panel-wave6-2004',
    'sipp-topical-2004panel-wave7-2004', 'sipp-topical-2004panel-wave8-2004',
    'sipp-topical-2008panel-wave1-2008', 'sipp-topical-2008panel-wave10-2008',
    'sipp-topical-2008panel-wave11-2008', 'sipp-topical-2008panel-wave13-2008',
    'sipp-topical-2008panel-wave2-2008', 'sipp-topical-2008panel-wave3-2008',
    'sipp-topical-2008panel-wave4-2008', 'sipp-topical-2008panel-wave5-2008',
    'sipp-topical-2008panel-wave6-2008', 'sipp-topical-2008panel-wave7-2008',
    'sipp-topical-2008panel-wave8-2008', 'sipp-topical-2008panel-wave9-2008',
    'sipp-topicaled-1990panel-wave5-1990', 'sipp-topicaled-1991panel-wave8-1991',
    'sipp-topicaled-1992panel-wave5-1992', 'sipp-topicaled-1992panel-wave8-1992',
    'sipp-topicaled-1993panel-wave5-1993', 'sipp-topicaled-1993panel-wave8-1993',
    'sipp-topicaledex-1990panel-wave5-1990', 'sipp-topicaledex-1990panel-wave8-1990',
    'sipp-topicaledex-1991panel-wave5-1991', 'sipp-topicaledex-1991panel-wave8-1991',
    'sipp-topicaledex-1992panel-wave5-1992', 'sipp-topicaledex-1992panel-wave8-1992',
    'sipp-topicaledex-1993panel-wave5-1993', 'sipp-topicaledex-1993panel-wave8-1993',
    'sipp-topicalex-1992panel-wave6-1992', 'sipp-topicalex-1993panel-wave3-1993',
    'sipp-topicalres-1990panel-wave5-1990', 'sipp-topicalres-1991panel-wave8-1991',
    'sipp-topicalres-1992panel-wave5-1992', 'sipp-topicalres-1992panel-wave8-1992',
    'sipp-topicalres-1993panel-wave5-1993', 'sipp-topicalres-1993panel-wave8-1993',
    'sipp-topicalres-2001panel-wave8-2001', 'surname-2000', 'surname-2010', 'timeseries-aies-basic',
    'timeseries-aies-ecom', 'timeseries-aies-exp01', 'timeseries-aies-exp02', 'timeseries-aies-inv',
    'timeseries-aies-miscsector', 'timeseries-asm-area2012', 'timeseries-asm-area2017',
    'timeseries-asm-benchmark2017', 'timeseries-asm-benchmark2022', 'timeseries-asm-industry',
    'timeseries-asm-product', 'timeseries-asm-state', 'timeseries-asm-value2012',
    'timeseries-asm-value2017', 'timeseries-bds', 'timeseries-eits-advm3', 'timeseries-eits-bfs',
    'timeseries-eits-ftd', 'timeseries-eits-ftdadv', 'timeseries-eits-hv', 'timeseries-eits-m3',
    'timeseries-eits-marts', 'timeseries-eits-mhs', 'timeseries-eits-mhs2', 'timeseries-eits-mrts',
    'timeseries-eits-mrtsadv', 'timeseries-eits-mtis', 'timeseries-eits-mwts',
    'timeseries-eits-mwtsadv', 'timeseries-eits-qfr', 'timeseries-eits-qpr', 'timeseries-eits-qss',
    'timeseries-eits-qtax', 'timeseries-eits-resconst', 'timeseries-eits-ressales',
    'timeseries-eits-vip', 'timeseries-govs', 'timeseries-govsemp', 'timeseries-govspension',
    'timeseries-govsschfin', 'timeseries-govsstatefin', 'timeseries-govsstatetax',
    'timeseries-healthins-sahie', 'timeseries-hhpulse', 'timeseries-hps', 'timeseries-idb-1year',
    'timeseries-idb-5year', 'timeseries-intltrade-exports-enduse',
    'timeseries-intltrade-exports-enduseexport', 'timeseries-intltrade-exports-hitech',
    'timeseries-intltrade-exports-hitechexport', 'timeseries-intltrade-exports-hs',
    'timeseries-intltrade-exports-hsexport', 'timeseries-intltrade-exports-naics',
    'timeseries-intltrade-exports-naicsexport', 'timeseries-intltrade-exports-porths',
    'timeseries-intltrade-exports-porthsexport', 'timeseries-intltrade-exports-sitc',
    'timeseries-intltrade-exports-sitcexport', 'timeseries-intltrade-exports-statehs',
    'timeseries-intltrade-exports-statehsexport', 'timeseries-intltrade-exports-statenaics',
    'timeseries-intltrade-exports-statenaicsexport', 'timeseries-intltrade-exports-usda',
    'timeseries-intltrade-exports-usdaexport', 'timeseries-intltrade-imports-enduse',
    'timeseries-intltrade-imports-enduseimport', 'timeseries-intltrade-imports-hitech',
    'timeseries-intltrade-imports-hitechimport', 'timeseries-intltrade-imports-hs',
    'timeseries-intltrade-imports-hsimport', 'timeseries-intltrade-imports-naics',
    'timeseries-intltrade-imports-naicsimport', 'timeseries-intltrade-imports-porths',
    'timeseries-intltrade-imports-porthsimport', 'timeseries-intltrade-imports-sitc',
    'timeseries-intltrade-imports-sitcimport', 'timeseries-intltrade-imports-statehs',
    'timeseries-intltrade-imports-statehsimport', 'timeseries-intltrade-imports-statenaics',
    'timeseries-intltrade-imports-statenaicsimport', 'timeseries-intltrade-imports-usda',
    'timeseries-intltrade-imports-usdaimport', 'timeseries-poverty-histpov2',
    'timeseries-poverty-saipe', 'timeseries-poverty-saipe-schdist', 'timeseries-pseo-earnings',
    'timeseries-pseo-flows', 'timeseries-qwi-rh', 'timeseries-qwi-sa', 'timeseries-qwi-se',
    'timeseries-soma', 'viusa-2021', 'viusc-2021', 'viuspuf-2021', 'zbp-1994', 'zbp-1995', 'zbp-1996',
    'zbp-1997', 'zbp-1998', 'zbp-1999', 'zbp-2000', 'zbp-2001', 'zbp-2002', 'zbp-2003', 'zbp-2004',
    'zbp-2005', 'zbp-2006', 'zbp-2007', 'zbp-2008', 'zbp-2009', 'zbp-2010', 'zbp-2011', 'zbp-2012',
    'zbp-2013', 'zbp-2014', 'zbp-2015', 'zbp-2016', 'zbp-2017', 'zbp-2018'
]

DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"us-census-bureau-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]

# One published Delta table per dataset. SELECT * passthrough: the 1758 column
# sets are distinct, undocumented, and string-typed, so a blind cast would be
# brittle. This stays a thin publish gate — DuckDB reads the NDJSON columns and
# a 0-row result fails the node.
TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{spec.id}-transform",
        deps=[spec.id],
        sql=f'SELECT * FROM "{spec.id}"',
    )
    for spec in DOWNLOAD_SPECS
]
