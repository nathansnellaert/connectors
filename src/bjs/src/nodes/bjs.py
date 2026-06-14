"""BJS connector — Bureau of Justice Statistics aggregate datasets.

Source: the Socrata-style REST API at https://api.ojp.gov/bjsdataset/v1/<resource_id>.json
covering 10 published BJS datasets in two groups:
  - NCVS Select (4 resources): personal/household victimization + population
    denominators. These are large incident-level survey tables (the population
    denominator tables run to several million rows each).
  - NIBRS National Estimates (6 resources): aggregated estimate tables keyed on
    indicator_name (counts, rates, offenses, victimizations).

Fetch shape: stateless full re-pull. There is no incremental/since filter on
this API and the datasets are revised annually, so we refetch the whole corpus
each run and overwrite. Every value is serialized as a string by Socrata and the
column set differs per resource, so raw is stored as NDJSON (no schema burden);
the per-resource SQL transforms re-type the few columns that matter.

Pagination: the API silently caps a query at 1000 rows unless $limit is given,
and several tables far exceed any single $limit (personal population ~6.3M rows).
So we page with $limit + $offset ordered by :id until a short/empty page, rather
than trusting one big $limit (which would silently truncate). Streamed to
ndjson.gz so the multi-million-row tables never sit fully in memory.
"""
import json

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

BASE = "https://api.ojp.gov/bjsdataset/v1/"
PAGE_SIZE = 50000
# Safety ceiling: 1000 pages * 50k = 50M rows. The biggest table today is ~6.3M
# rows (127 pages). If a fetch ever exhausts this it means the source grew far
# past expectations — raise rather than silently truncate.
MAX_PAGES = 1000
STATE_VERSION = 1

# Two collection groups with distinct schemas — drives which columns each
# transform re-types. NCVS tables all carry `year`; NIBRS tables carry
# `indicator_name` + `estimate`.
NCVS_IDS = ["gcuy-rt5g", "gkck-euys", "r4j4-fdwx", "ya4e-n9zp"]
NIBRS_IDS = ["iv7i-eah6", "kj7p-vx4s", "ms42-n765", "r32q-bdaw", "uy37-xgmh", "x3sz-eb6y"]
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
    total = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as fh:
        for page in range(MAX_PAGES):
            rows = _fetch_page(resource_id, page * PAGE_SIZE)
            if not rows:
                break
            for row in rows:
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
            total += len(rows)
            if page % 10 == 0:
                print(f"{asset}: {total} rows fetched (page {page})", flush=True)
            if len(rows) < PAGE_SIZE:
                break
        else:
            # Loop ran the full MAX_PAGES without a short/empty page.
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
# NCVS tables get `year` cast to INTEGER (and drop year-null junk); NIBRS tables
# get `estimate` cast to DOUBLE (and drop indicator-null junk). All other columns
# pass through as-is (Socrata serializes everything as strings).
TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"bjs-{eid}-transform",
        deps=[f"bjs-{eid}"],
        sql=(
            f'SELECT * REPLACE (TRY_CAST(year AS INTEGER) AS year) '
            f'FROM "bjs-{eid}" WHERE year IS NOT NULL'
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
