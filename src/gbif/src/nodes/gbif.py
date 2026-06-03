"""GBIF download step — one NodeSpec per collect entity.

Four entities, three distinct fetch surfaces (picked per the research handoff
by *what* is being collected):

  * occurrences — the ~3.8B-record corpus. AWS Open Data S3 snapshot
    (the chosen mechanism): date-partitioned Parquet at
    s3://gbif-open-data-us-east-1/occurrence/<YYYY-MM-DD>/occurrence.parquet/<NNNNNN>.
    Anonymous (--no-sign-request). 8000+ part files per snapshot, ~22MB /
    ~212k rows each. This is a genuine firehose: it cannot finish in one run,
    so it is fetched in batches with a part-index watermark in state and a
    soft per-run time budget. Backfill = a sequence of bounded refreshes.

  * datasets (~122k) and literature (~61k) — registry / bibliographic
    metadata via the REST API (https://api.gbif.org/v1/). Heavily nested,
    drifty records → streamed to NDJSON. The /dataset registry listing DOES
    support deep paging past offset 100000; /literature/search is under the
    cap. Both re-pull in full each run (stateless) — the corpus fits in one
    run and full re-pull picks up revisions for free.

  * species — the GBIF backbone taxonomy. NOTE: research suggested the REST
    /species endpoint, but probing showed /species hard-caps offset at
    100000 ("Offset is limited for this operation to 100000"), even with a
    datasetKey filter — so the 49.8M name corpus is NOT extractable via REST
    pagination. We instead pull the canonical bulk backbone export
    (simple.txt.gz, ~488MB, tab-delimited, the taxonomy that actually joins
    to occurrences via taxonKey). It is fetched via HTTP Range requests so
    memory stays bounded, and saved as the raw gzip bytes.

Incremental support: none usable. REST has no since/modifiedAfter; S3
snapshots are full monthly re-publications. Occurrences resume across runs
via a snapshot+part-index watermark; the rest re-pull fully each refresh.
"""
import json
import time

import httpx
import pyarrow.parquet as pq
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from subsets_utils import (
    NodeSpec,
    get,
    load_state,
    save_state,
    raw_writer,
    raw_parquet_writer,
)

# Bump when the shape of any persisted watermark changes — forces stale state
# to be discarded on the next run rather than silently misinterpreted.
STATE_VERSION = 1

# --- REST (datasets, literature) ---------------------------------------------
REST_BASE = "https://api.gbif.org/v1"
PAGE_SIZE = 1000                 # API maximum per page
MAX_PAGES = 5000                 # safety ceiling (~123 pages expected); raises if hit

# --- species backbone bulk export --------------------------------------------
BACKBONE_URL = "https://hosted-datasets.gbif.org/datasets/backbone/current/simple.txt.gz"
RANGE_CHUNK = 32 * 1024 * 1024   # 32MB HTTP Range windows

# --- occurrences S3 snapshot firehose ----------------------------------------
OCC_BUCKET = "gbif-open-data-us-east-1"
OCC_PREFIX = "occurrence"
PARTS_PER_BATCH = 8              # ~8 x 22MB parts -> ~175MB compressed per batch file
MAX_FETCH_SECONDS = 600         # soft per-run budget; resumes next refresh via watermark


# =============================================================================
# Transport — retry/backoff with an honest transient predicate
# =============================================================================
_TRANSIENT_HTTPX = (
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
    httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError, httpx.ProxyError,
)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_HTTPX):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    # Genuinely-missing / permission errors are permanent — never retry them.
    if isinstance(exc, (FileNotFoundError, PermissionError, IsADirectoryError)):
        return False
    # s3fs / fsspec surface transient S3 issues (throttling, reset connections)
    # as OSError / ConnectionError / TimeoutError subclasses.
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    return False


_RETRY = dict(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=2, max=60),
    reraise=True,
)


@retry(**_RETRY)
def _get_json(url: str, params: dict) -> dict:
    resp = get(url, params=params, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


@retry(**_RETRY)
def _get_range(url: str, start: int, end: int) -> httpx.Response:
    resp = get(url, headers={"Range": f"bytes={start}-{end}"}, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp


@retry(**_RETRY)
def _s3_ls(fs, path: str) -> list:
    return fs.ls(path)


@retry(**_RETRY)
def _read_s3_table(fs, key: str):
    with fs.open(key, "rb") as fh:
        return pq.read_table(fh)


# =============================================================================
# datasets / literature — paginated REST -> streamed NDJSON (full re-pull)
# =============================================================================
def _paginate_to_ndjson(node_id: str, url: str) -> None:
    asset = node_id
    written = 0
    offset = 0
    pages = 0
    with raw_writer(asset, "ndjson.gz", mode="wt", compression="gzip") as fh:
        while True:
            data = _get_json(url, {"limit": PAGE_SIZE, "offset": offset})
            results = data.get("results", [])
            for rec in results:
                fh.write(json.dumps(rec, ensure_ascii=False))
                fh.write("\n")
            written += len(results)
            pages += 1
            if pages % 25 == 0:
                print(f"  {asset}: page {pages}, {written} records (offset={offset})", flush=True)
            if data.get("endOfRecords") or not results:
                break
            offset += PAGE_SIZE
            if pages >= MAX_PAGES:
                raise RuntimeError(
                    f"{asset}: exceeded MAX_PAGES={MAX_PAGES} at offset {offset} — "
                    "source grew past expectations, investigate before raising the cap"
                )
    save_state(asset, {
        "schema_version": STATE_VERSION,
        "last_run_stats": {"records": written, "pages": pages},
    })
    print(f"  {asset}: done — {written} records across {pages} pages", flush=True)


def fetch_datasets(node_id: str) -> None:
    _paginate_to_ndjson(node_id, f"{REST_BASE}/dataset")


def fetch_literature(node_id: str) -> None:
    _paginate_to_ndjson(node_id, f"{REST_BASE}/literature/search")


# =============================================================================
# species — bulk backbone export pulled via HTTP Range, saved as raw gzip bytes
# =============================================================================
def fetch_species(node_id: str) -> None:
    asset = node_id
    start = 0
    written = 0
    total = None
    with raw_writer(asset, "txt.gz", mode="wb", compression=None) as fh:
        while True:
            resp = _get_range(BACKBONE_URL, start, start + RANGE_CHUNK - 1)
            chunk = resp.content
            fh.write(chunk)
            written += len(chunk)
            content_range = resp.headers.get("content-range")
            # Server returned the whole body (no Range support) — done.
            if resp.status_code == 200 or not content_range:
                break
            total = int(content_range.split("/")[-1])
            start += len(chunk)
            if not chunk or start >= total:
                break
            if written % (RANGE_CHUNK * 4) == 0:
                pct = f"{100 * written / total:.0f}%" if total else "?"
                print(f"  {asset}: {written} bytes ({pct})", flush=True)
    save_state(asset, {
        "schema_version": STATE_VERSION,
        "last_run_stats": {"bytes": written, "total": total},
    })
    print(f"  {asset}: downloaded {written} bytes (declared total {total})", flush=True)


# =============================================================================
# occurrences — S3 Parquet snapshot firehose (batched, part-index watermark)
# =============================================================================
def _latest_snapshot(fs) -> str:
    dated = []
    for entry in _s3_ls(fs, f"{OCC_BUCKET}/{OCC_PREFIX}"):
        name = entry.rstrip("/").split("/")[-1]
        if len(name) == 10 and name[:4].isdigit() and name[4] == "-" and name[7] == "-":
            dated.append(name)
    if not dated:
        raise RuntimeError(f"no dated occurrence snapshots under s3://{OCC_BUCKET}/{OCC_PREFIX}/")
    return max(dated)


def fetch_occurrences(node_id: str) -> None:
    import s3fs  # heavy import; only this spec needs it

    asset = node_id
    state = load_state(asset)
    if state.get("schema_version") != STATE_VERSION:
        state = {}  # unknown/legacy state — start clean

    fs = s3fs.S3FileSystem(anon=True)
    snapshot = _latest_snapshot(fs)
    # A new monthly snapshot is a full re-publication — restart from part 0.
    if state.get("snapshot") != snapshot:
        state = {"schema_version": STATE_VERSION, "snapshot": snapshot, "next_part": 0}

    part_dir = f"{OCC_BUCKET}/{OCC_PREFIX}/{snapshot}/occurrence.parquet"
    parts = sorted(
        p for p in _s3_ls(fs, part_dir)
        if p.rstrip("/").split("/")[-1].isdigit()
    )
    total = len(parts)
    if total == 0:
        raise RuntimeError(f"{asset}: no parquet part files under s3://{part_dir}/")

    # Writer schema is fixed across the snapshot; cast every part to it so the
    # streaming writer never sees drift (and fails loudly if a part genuinely
    # differs in column types).
    schema = _read_s3_table(fs, parts[0]).schema.remove_metadata()

    next_part = state.get("next_part", 0)
    deadline = time.time() + MAX_FETCH_SECONDS
    batches = 0
    rows_written = 0

    while next_part < total:
        if time.time() > deadline:
            print(f"  {asset}: time budget reached at part {next_part}/{total}", flush=True)
            break
        lo = next_part
        hi = min(lo + PARTS_PER_BATCH, total)
        batch_key = f"{lo:06d}-{hi - 1:06d}"   # pure batch coordinate, no slug/entity
        batch_asset = f"{asset}-{batch_key}"

        # Write raw FIRST; advance the watermark only after the file is closed.
        with raw_parquet_writer(batch_asset, schema) as writer:
            for i in range(lo, hi):
                table = _read_s3_table(fs, parts[i])
                if not table.schema.equals(schema, check_metadata=False):
                    table = table.cast(schema)
                else:
                    table = table.replace_schema_metadata(None)
                writer.write_table(table)
                rows_written += table.num_rows

        next_part = hi
        batches += 1
        save_state(asset, {
            "schema_version": STATE_VERSION,
            "snapshot": snapshot,
            "next_part": next_part,
            "total_parts": total,
            "last_run_stats": {"batches_this_run": batches, "rows_this_run": rows_written},
        })
        print(f"  {asset}: wrote {batch_asset} ({hi}/{total} parts, {rows_written} rows this run)", flush=True)

    print(f"  {asset}: snapshot={snapshot} progress {next_part}/{total} parts", flush=True)


# =============================================================================
# Specs — one per entity-union entry: datasets, literature, occurrences, species
# =============================================================================
DOWNLOAD_SPECS = [
    NodeSpec(id="gbif-datasets",    fn=fetch_datasets,    kind="download"),
    NodeSpec(id="gbif-literature",  fn=fetch_literature,  kind="download"),
    NodeSpec(id="gbif-occurrences", fn=fetch_occurrences, kind="download"),
    NodeSpec(id="gbif-species",     fn=fetch_species,     kind="download"),
]
