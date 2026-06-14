"""BTS (Bureau of Transportation Statistics) — Socrata open-data catalog.

Catalog connector. Every entity is a stable four-four dataset id on
data.bts.gov, fetched in full each refresh from the Socrata resource
endpoint (`/resource/<id>.json`) with $limit/$offset pagination.

Strategy: stateless full re-pull. Each dataset is a full snapshot; we
overwrite it every run, so revisions and late corrections are picked up
for free. No incremental watermark — the source's $where filters exist
but our pattern is full-corpus snapshot per refresh.

Raw shape: Socrata returns a list of records with all-string values and
columns that drift (sparse wide tables omit null fields per row). That is
the textbook NDJSON case, so we stream gzip'd NDJSON. To keep the schema
stable for DuckDB's `read_json_auto` (which samples leading rows and would
miss columns that only appear later in a wide sparse table), each row is
projected onto the dataset's full documented column set from the metadata
view (`/api/views/<id>.json` -> columns[].fieldName); missing fields become
null. The published table therefore matches the documented schema.

A few datasets are large (j246-y2rf ~26M rows, w96p-f2qv ~6M), so we
stream-write page by page and never hold more than one page in memory.
"""

import json
import os

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
    configure_http,
    get,
    raw_writer,
    save_state,
)

# Entity union — the authoritative coverage target (72 BTS four-four ids).
ENTITY_IDS = [
    "2ydv-qfge", "33xp-y9fx", "3qgg-2u2a", "3w2s-iysp", "3xj5-daif",
    "56rv-9p75", "5rpz-kgm9", "5yqg-88j3", "63me-zi7c", "6aiz-ybqx",
    "6cfa-ipzd", "7m5x-ubud", "7mzw-a8si", "8cjz-h8bz", "9tn7-rkk2",
    "amn9-4jcb", "anet-6eas", "as3z-m8rd", "b3ps-driu", "bqx9-a7yw",
    "bu82-4pwz", "bw6n-ddqk", "ca7h-i9yt", "cqdc-cm7d", "crem-w557",
    "cvai-skrf", "d2st-9nd6", "dkgi-gbeh", "e5cn-ri8q", "em9t-xx9j",
    "f3sb-gw7h", "g3h6-334u", "ggca-ddee", "gjp5-nh2u", "gn77-pp24",
    "h2kz-rw8a", "h77j-murt", "hinw-eisy", "hpjf-p2n3", "iyde-f8k7",
    "iyz9-3pd9", "j246-y2rf", "j6uy-twhg", "jn4u-gqv9", "jtvy-isaj",
    "kbvr-tyu5", "kdtd-3e96", "ke6h-ga46", "keg4-3bc2", "kxxg-a7c6",
    "m2bh-93w3", "mwaz-n68f", "navd-gpqa", "nu8j-7gmn", "pqmc-mnds",
    "q4tb-tbff", "qq62-cjjy", "r495-tyji", "swpm-impx", "tcq5-4pgu",
    "u2pk-kyws", "u3uh-j5wt", "uhb6-dvuq", "va72-z8hz", "w3m5-t2w3",
    "w8ea-nba4", "w96p-f2qv", "wgzf-9czk", "xkuc-f3hj", "xnub-2sc4",
    "xrt2-b7j8", "y5ut-ibwt",
]

STATE_VERSION = 1

_RESOURCE = "https://data.bts.gov/resource/{eid}.json"
_VIEW = "https://data.bts.gov/api/views/{eid}.json"

PAGE_SIZE = 50000          # Socrata max page size for /resource
# Safety ceiling: largest known dataset is ~26M rows (~524 pages). 2000 pages
# (~100M rows) is comfortably above that; blowing past it means the source grew
# unexpectedly and we want a loud failure, not silent truncation.
MAX_PAGES = 2000


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


def _configure_token() -> None:
    """Attach a Socrata app token if one is in the environment — raises the
    shared throttling quota for the full enumeration pass. Optional; the API
    works unauthenticated. ASCII-only header value."""
    token = os.environ.get("SOCRATA_APP_TOKEN")
    if token:
        configure_http(headers={"X-App-Token": token})


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_json(url: str, params: dict | None = None):
    resp = get(url, params=params, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp.json()


def _field_names(eid: str) -> list[str]:
    """Documented column set for a dataset, in declared order. Drops Socrata
    system/computed columns (fieldName starting with ':')."""
    meta = _fetch_json(_VIEW.format(eid=eid))
    names = []
    seen = set()
    for col in meta.get("columns", []):
        fn = col.get("fieldName")
        if fn and not fn.startswith(":") and fn not in seen:
            seen.add(fn)
            names.append(fn)
    if not names:
        raise ValueError(f"{eid}: metadata view exposed no usable columns")
    return names


def fetch_one(node_id: str) -> None:
    asset = node_id                       # the spec id IS the asset name
    eid = node_id[len("bts-"):]           # recover the four-four id
    _configure_token()

    fields = _field_names(eid)

    total = 0
    pages = 0
    offset = 0
    # Stream gzip'd NDJSON, projecting every row onto the full documented
    # column set so the on-disk schema is stable for read_json_auto.
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as out:
        while True:
            if pages >= MAX_PAGES:
                raise RuntimeError(
                    f"{asset}: exceeded MAX_PAGES={MAX_PAGES} at offset {offset} "
                    f"({total} rows) — dataset grew past the safety ceiling"
                )
            rows = _fetch_json(
                _RESOURCE.format(eid=eid),
                params={"$order": ":id", "$limit": PAGE_SIZE, "$offset": offset},
            )
            if not rows:
                break
            for row in rows:
                rec = {fn: row.get(fn) for fn in fields}
                out.write(json.dumps(rec, separators=(",", ":")) + "\n")
            total += len(rows)
            pages += 1
            if pages % 20 == 0:
                print(f"{asset}: {total} rows ({pages} pages)", flush=True)
            if len(rows) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    # Raw written first; state is observability only (full re-pull, no watermark).
    save_state(asset, {
        "schema_version": STATE_VERSION,
        "last_run_stats": {"records": total, "pages": pages, "columns": len(fields)},
    })
    print(f"{asset}: done, {total} rows across {len(fields)} columns", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bts-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]


# One published Delta table per dataset. Thin pass-through: the raw NDJSON
# already carries the documented, schema-stable columns, so the transform's
# job is to expose it as a table and act as the non-empty-rows correctness gate.
TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{spec.id}-transform",
        deps=[spec.id],
        sql=f'SELECT * FROM "{spec.id}"',
    )
    for spec in DOWNLOAD_SPECS
]
