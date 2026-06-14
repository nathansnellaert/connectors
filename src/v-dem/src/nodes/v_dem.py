"""V-Dem connector — bulk GitHub .RData downloads.

Access strategy (from research, mechanism `bulk_github_rdata`): the official
vdeminstitute/vdemdata R package commits the full V-Dem corpus as gzipped
R-serialized .RData files, served as direct no-auth raw.githubusercontent.com
downloads. Two entities are published:

  - vdem   : Country-Year V-Dem Full+Others. Wide panel, ~28k polity-years x
             ~4600 indicator/index/supplementary columns (~34MB .RData).
  - vparty : V-Party party-level data, ~12k party-elections x ~384 columns
             (~3.4MB .RData).

Format is RData, NOT CSV — decoded with pyreadr into a pandas DataFrame. URLs
point at the master branch and are updated in place on each annual release
(v16 / March 2026), so the correct refresh policy is a full re-pull every run
(shape 1: stateless full snapshot). The corpus easily re-fetches in a couple of
minutes; there is no incremental/delta filter and no need for one. Freshness
gating (re-fetch cadence) is the maintain step's job, not ours.

Raw format: parquet. Each file is a single, full-snapshot wide table with
columns whose types are stable per column (the indicators are all doubles; a
handful of identity columns are strings). Because the tables carry thousands of
columns, an explicit hand-written pa.schema() literal is impractical and
unnecessary here — there is exactly ONE full-snapshot write per asset (no
batched/repeated writes where an up-front schema contract earns its keep), so we
let pyarrow infer types from the decoded DataFrame after coercing the few
object-dtype columns (e.g. historical_date) to strings, which makes the parquet
schema fully deterministic across releases.
"""
import tempfile

import httpx
import pyarrow as pa
import pyreadr
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, save_raw_parquet

# entity_id -> filename in the repo's data/ directory. Stable master-branch URLs,
# updated in place each annual release; re-pull rather than pinning a tag.
_BASE = "https://raw.githubusercontent.com/vdeminstitute/vdemdata/master/data"
_FILES = {
    "vdem": "vdem.RData",
    "vparty": "vparty.RData",
}

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
def _download(url: str) -> bytes:
    # raw.githubusercontent.com serves large bodies; allow a generous read timeout.
    resp = get(url, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp.content


def fetch_one(node_id: str) -> None:
    """Download one V-Dem .RData file, decode it, write a parquet raw asset."""
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity = node_id[len("v-dem-"):]
    filename = _FILES[entity]  # KeyError here is a bug, not a transient — let it raise
    url = f"{_BASE}/{filename}"

    content = _download(url)
    print(f"{asset}: downloaded {len(content)} bytes from {url}", flush=True)

    # pyreadr reads from a path, so stage the gzip-wrapped RData to a temp file.
    with tempfile.NamedTemporaryFile(suffix=".RData") as tf:
        tf.write(content)
        tf.flush()
        objects = pyreadr.read_r(tf.name)

    # Each file holds a single named R object (the dataframe of the same name).
    if entity not in objects:
        # Fail loudly if the bundled object name ever changes — better than
        # silently publishing whichever object happens to be first.
        raise AssertionError(
            f"{asset}: expected R object '{entity}', got {list(objects.keys())}"
        )
    df = objects[entity]

    # Coerce object-dtype columns (dates, occasionally mixed numeric/string) to
    # plain strings so pyarrow infers a deterministic schema. NaN -> null.
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    table = pa.Table.from_pandas(df, preserve_index=False)
    print(
        f"{asset}: decoded {table.num_rows} rows x {table.num_columns} columns",
        flush=True,
    )
    save_raw_parquet(table, asset)


DOWNLOAD_SPECS = [
    NodeSpec(id="v-dem-vdem", fn=fetch_one, kind="download"),
    NodeSpec(id="v-dem-vparty", fn=fetch_one, kind="download"),
]

# One published Delta table per subset. The raw parquet already carries good
# per-column types from the RData decode, so the transform is a thin pass:
# pass every indicator/index column through, drop rows with no year (the panel
# key), and the runtime overwrites the named Delta table. A full snapshot per
# refresh means no dedup is needed.
TRANSFORM_SPECS = [
    SqlNodeSpec(
        id="v-dem-vdem-transform",
        deps=["v-dem-vdem"],
        sql='''
            SELECT *
            FROM "v-dem-vdem"
            WHERE year IS NOT NULL
              AND country_id IS NOT NULL
        ''',
    ),
    SqlNodeSpec(
        id="v-dem-vparty-transform",
        deps=["v-dem-vparty"],
        sql='''
            SELECT *
            FROM "v-dem-vparty"
            WHERE year IS NOT NULL
              AND v2paid IS NOT NULL
        ''',
    ),
]
