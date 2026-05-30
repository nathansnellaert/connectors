"""BTS download — Socrata open-data platform (data.bts.gov).

Mechanism: socrata. Every BTS dataset has a stable four-four id (e.g.
`crem-w557`) and is fetchable in full from
`https://data.bts.gov/resource/<four-four>.json`. SoQL offset pagination
(`$limit`/`$offset`, max page 50000) walks the whole table; `$order=:id`
pins a stable row order so pages don't overlap or skip as offset advances.

Shape: stateless full re-pull per refresh (shape 1). BTS datasets are
full-corpus snapshots — research's handoff explicitly says "ignore
incremental and re-pull". Most datasets are small (tens to thousands of
rows); a handful are large (one is ~26M rows), so every fetch STREAMS
pages straight to a gzipped NDJSON file via `raw_writer` rather than
accumulating the whole table in RAM — at most one 50k-row page is held in
memory at a time.

Raw format: NDJSON. The 72 datasets have wildly heterogeneous, wide, and
drifting schemas (one has 136 columns of mixed types; others carry nested
Socrata location/point objects). A single parquet schema can't span them,
so each dataset's records are written verbatim as line-delimited JSON and
re-typed downstream by transform.

State: none gates the fetch (freshness is the maintain step's job). A
small `last_run_stats` blob is written purely for observability — it never
short-circuits a run.
"""
import json
import time

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, raw_writer, save_state

# The entity union — the authoritative coverage target. One spec per id.
# Copied from
# data/sources/bts/steps/5c6323c1f826454aaf01c1f2c9ad41bb/entity_union.json
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

RESOURCE_URL = "https://data.bts.gov/resource/{four_four}.json"
PAGE_SIZE = 50000          # Socrata documented max page size
# Safety ceiling: largest dataset today is ~26M rows -> ~525 pages. 4000
# pages == 200M rows is far past any current dataset; tripping it means a
# dataset grew unexpectedly (or pagination is looping), so we raise rather
# than truncate silently.
MAX_PAGES = 4000
LOG_EVERY_PAGES = 20       # ~a few minutes of wall-clock at deep offsets


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
def _fetch_page(four_four: str, offset: int) -> list:
    """Fetch one SoQL page. Transient errors retried; permanent 4xx raise."""
    url = RESOURCE_URL.format(four_four=four_four)
    resp = get(
        url,
        params={"$order": ":id", "$limit": PAGE_SIZE, "$offset": offset},
        timeout=(10.0, 180.0),  # (connect, read) — large pages stream slowly
    )
    resp.raise_for_status()
    return resp.json()


def fetch_one(node_id: str) -> None:
    """Download one BTS dataset in full as gzipped NDJSON.

    The runtime passes the spec id; it IS the asset name. The four-four
    Socrata id is the suffix after the `bts-` prefix (four-four ids are
    already lowercase and contain no underscores, so no further mapping is
    needed).
    """
    asset = node_id
    four_four = node_id[len("bts-"):]

    total = 0
    pages = 0
    offset = 0
    # Stream pages straight to disk so we never hold the whole table in RAM.
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as f:
        while True:
            if pages >= MAX_PAGES:
                raise RuntimeError(
                    f"{asset}: exceeded MAX_PAGES={MAX_PAGES} "
                    f"(offset={offset}); dataset grew past expectations "
                    f"or pagination is not terminating"
                )
            rows = _fetch_page(four_four, offset)
            pages += 1
            for row in rows:
                f.write(json.dumps(row, separators=(",", ":")))
                f.write("\n")
            total += len(rows)
            if pages % LOG_EVERY_PAGES == 0:
                print(
                    f"  {asset}: {total:,} rows after {pages} pages "
                    f"(offset={offset})",
                    flush=True,
                )
            # A short page (fewer than requested) is the last page.
            if len(rows) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

    print(f"  {asset}: fetched {total:,} rows in {pages} pages", flush=True)

    # Observability only — never gates whether this fn runs (maintain does).
    save_state(asset, {
        "schema_version": 1,
        "last_run_stats": {
            "records": total,
            "pages": pages,
            "finished_at": int(time.time()),
        },
    })


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bts-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]
