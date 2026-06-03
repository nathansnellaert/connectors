"""Gapminder download — open-numbers DDFcsv repos as per-repo long-format tables.

Two collect entities, one repo each:

  - systema-globalis-datapoints -> open-numbers/ddf--gapminder--systema_globalis
  - fasttrack-datapoints        -> open-numbers/ddf--gapminder--fasttrack

Both repos use the DDFcsv layout: a root ``datapackage.json`` manifest catalogs
every resource, and each indicator is one narrow CSV
(``ddf--datapoints--<indicator>--by--<dim>--time.csv``) with columns
``<dim>, time, <indicator>``. The dimension column varies across resources
(geo / country / global / world_4region / income_groups, plus two
``geo,gender,time`` multidimensional resources in systema_globalis), and the
single non-key field is always the indicator's value column.

We fold every datapoint CSV of a repo into ONE long-format raw table with a
fixed all-string schema (geo, time, indicator, value, dimension, extra_dims),
so a heterogeneous corpus of ~480 indicators lands as a single stable parquet.
Values and time tokens are preserved as their exact source text — typing is
deferred to transform, which avoids float-repr drift and copes with DDF time
that need not be a plain year.

Fetch shape: stateless full re-pull. The source exposes no incremental delta
filter — raw URLs pin to master and content mutates in place on upstream
refresh — so each run re-fetches the whole corpus and overwrites. At ~480 / ~370
small CSVs over rate-limit-free raw.githubusercontent.com this is a few minutes
per entity; cheaper than the bookkeeping a watermark would need (and there is no
watermark to track). Enumeration uses datapackage.json (the contents API caps
unauthenticated callers at 60/hour; the manifest is a single request).
"""
import io
import json

import httpx
import pyarrow as pa
import pyarrow.csv as pacsv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, raw_parquet_writer

# entity id (collect slug) -> open-numbers GitHub repo
ENTITY_REPOS = {
    "systema-globalis-datapoints": "ddf--gapminder--systema_globalis",
    "fasttrack-datapoints": "ddf--gapminder--fasttrack",
}

# Fixed long-format contract. Everything is string: the corpus mixes int- and
# float-valued indicators (and a handful of categorical ones), and DDF time is
# not guaranteed to be a plain year — strings preserve exact source text and
# keep the parquet schema stable across all ~480 indicator CSVs.
SCHEMA = pa.schema([
    ("geo", pa.string()),         # primary dimension member id (e.g. "afg", "world")
    ("time", pa.string()),        # raw DDF time token
    ("indicator", pa.string()),   # indicator id == the value column name
    ("value", pa.string()),       # raw value as source text
    ("dimension", pa.string()),   # name of the primary geo dimension column
    ("extra_dims", pa.string()),  # JSON of any extra dimensions (e.g. gender), "" when none
])

_RAW_BASE = "https://raw.githubusercontent.com/open-numbers/{repo}/master"

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
    wait=wait_exponential(min=2, max=60),
    reraise=True,
)
def _fetch_json(url: str):
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=2, max=60),
    reraise=True,
)
def _fetch_bytes(url: str) -> bytes:
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.content


def _datapoint_resources(manifest: dict) -> list[dict]:
    """Datapoint resources from a DDFcsv datapackage.json (path contains 'datapoints')."""
    return [
        res for res in manifest.get("resources", [])
        if "datapoints" in res.get("path", "")
    ]


def _csv_to_long(content: bytes, res: dict) -> pa.Table | None:
    """Parse one datapoint CSV into the fixed long-format schema.

    The resource schema gives column names and the primaryKey; the single
    non-key field is the indicator value column, the non-time key column(s)
    are the geo dimension(s).
    """
    sch = res["schema"]
    pk = sch.get("primaryKey", [])
    field_names = [f["name"] for f in sch["fields"]]
    value_cols = [f for f in field_names if f not in pk]
    # DDF datapoints carry exactly one measure; anything else is a manifest bug.
    assert len(value_cols) == 1, f"{res['path']}: expected 1 value col, got {value_cols}"
    value_col = value_cols[0]
    dim_cols = [c for c in pk if c != "time"]
    assert dim_cols, f"{res['path']}: no geo dimension in primaryKey {pk}"
    primary_dim = dim_cols[0]
    extra_cols = dim_cols[1:]

    # Force every column to string so exact source text is preserved and the
    # batch always matches SCHEMA regardless of the indicator's native type.
    tbl = pacsv.read_csv(
        io.BytesIO(content),
        convert_options=pacsv.ConvertOptions(
            column_types={n: pa.string() for n in field_names}
        ),
    )
    n = tbl.num_rows
    if n == 0:
        return None

    if extra_cols:
        extra_rows = [
            {c: tbl.column(c)[i].as_py() for c in extra_cols} for i in range(n)
        ]
        extra_arr = pa.array([json.dumps(d) for d in extra_rows], type=pa.string())
    else:
        extra_arr = pa.array([""] * n, type=pa.string())

    return pa.table(
        {
            "geo": tbl.column(primary_dim),
            "time": tbl.column("time"),
            "indicator": pa.array([value_col] * n, type=pa.string()),
            "value": tbl.column(value_col),
            "dimension": pa.array([primary_dim] * n, type=pa.string()),
            "extra_dims": extra_arr,
        },
        schema=SCHEMA,
    )


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity_id = node_id[len("gapminder-"):]
    repo = ENTITY_REPOS[entity_id]
    base = _RAW_BASE.format(repo=repo)

    manifest = _fetch_json(f"{base}/datapackage.json")
    resources = _datapoint_resources(manifest)
    total = len(resources)
    print(f"[{asset}] {repo}: {total} datapoint resources", flush=True)

    rows_written = 0
    files_written = 0
    with raw_parquet_writer(asset, SCHEMA) as writer:
        for i, res in enumerate(resources, 1):
            url = f"{base}/{res['path']}"
            content = _fetch_bytes(url)
            table = _csv_to_long(content, res)
            if table is not None and table.num_rows:
                writer.write_table(table)
                rows_written += table.num_rows
                files_written += 1
            if i % 50 == 0:
                print(f"[{asset}] {i}/{total} files, {rows_written} rows", flush=True)

    print(
        f"[{asset}] done: {files_written}/{total} non-empty files, {rows_written} rows",
        flush=True,
    )


DOWNLOAD_SPECS = [
    NodeSpec(id="gapminder-systema-globalis-datapoints", fn=fetch_one, kind="download"),
    NodeSpec(id="gapminder-fasttrack-datapoints", fn=fetch_one, kind="download"),
]
