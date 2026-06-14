"""UNESCO Institute for Statistics (UIS) — implement step.

Source corpus is delivered via the Bulk Data Download Service (BDDS): a small
number of date-partitioned ZIP archives, each holding normalized long-format
CSV that mirrors the entire UIS database. Per the research handoff the two
archives that together cover the whole corpus are:

  - SDG.zip  — SDG 4 Education global & thematic indicators
  - OPRI.zip — Other Policy Relevant Indicators (ex-NATMON)

Each archive contains the same file layout:
  {P}_LABEL.csv          INDICATOR_ID, INDICATOR_LABEL_EN        (indicator catalog)
  {P}_DATA_NATIONAL.csv  INDICATOR_ID, COUNTRY_ID, YEAR, VALUE, MAGNITUDE, QUALIFIER
  {P}_DATA_REGIONAL.csv  (region-level — geo unit is a region label, not a country)
  {P}_METADATA.csv       per-observation source notes (very large; not published)
  {P}_COUNTRY.csv / {P}_REGION.csv  geo reference tables

Two collect entities are published:
  - indicators : union of the LABEL files (one row per indicator).
  - values     : union of the DATA_NATIONAL files (country x indicator x year).
                 Regional aggregates are intentionally excluded to keep a clean
                 country-level long table; re-add as a separate subset if needed.

The bdds/<YYYYMM>/ path segment changes each release and is NOT persistent, so
each fetch discovers the current archive URLs from the public bulk listing page.
Releases are ~twice-yearly; we re-fetch the full corpus each run (stateless full
re-pull) — there is no incremental query for the bulk files and the corpus is a
few hundred MB of CSV, cheap to re-pull. Freshness gating is the maintain step's
job, not ours.
"""

import io
import re
import zipfile

import httpx
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
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
    save_raw_parquet,
)

# --- source constants -------------------------------------------------------

BULK_LISTING_URL = "https://databrowser.uis.unesco.org/resources/bulk"
ARCHIVES = ("SDG", "OPRI")  # the two BDDS archives covering the full corpus

# DATA_NATIONAL columns are uppercase in both archives; we append DATASET.
VALUES_SCHEMA = pa.schema([
    ("INDICATOR_ID", pa.string()),
    ("COUNTRY_ID", pa.string()),
    ("YEAR", pa.int32()),
    ("VALUE", pa.float64()),
    ("MAGNITUDE", pa.string()),
    ("QUALIFIER", pa.string()),
    ("DATASET", pa.string()),
])

INDICATORS_SCHEMA = pa.schema([
    ("INDICATOR_ID", pa.string()),
    ("INDICATOR_LABEL_EN", pa.string()),
    ("DATASET", pa.string()),
])

# 32 MB CSV read blocks: a handful of bounded-memory batches per archive
# rather than one ~200 MB arrow table held whole.
_CSV_BLOCK_SIZE = 32 * 1024 * 1024


# --- HTTP with retry --------------------------------------------------------

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
def _http_get(url: str, *, read_timeout: float = 120.0) -> httpx.Response:
    resp = get(url, timeout=(10.0, read_timeout))
    resp.raise_for_status()
    return resp


def _discover_archive_urls() -> dict:
    """Scrape the bulk listing page for the current BDDS archive URLs.

    The page embeds the download URLs as JSON. We pick the highest release
    month seen for each archive name so a stale archived link can't win.
    """
    resp = _http_get(BULK_LISTING_URL, read_timeout=60.0)
    html = resp.text
    urls = {}
    for name in ARCHIVES:
        pattern = (
            r"https://download\.uis\.unesco\.org/bdds/(\d{6})/"
            + re.escape(name)
            + r"\.zip"
        )
        months = [m.group(1) for m in re.finditer(pattern, html)]
        if not months:
            raise RuntimeError(
                f"Could not find {name}.zip on bulk listing {BULK_LISTING_URL}"
            )
        latest = max(months)
        urls[name] = (
            f"https://download.uis.unesco.org/bdds/{latest}/{name}.zip"
        )
    return urls


def _open_archive(url: str) -> zipfile.ZipFile:
    resp = _http_get(url, read_timeout=600.0)
    return zipfile.ZipFile(io.BytesIO(resp.content))


# --- fetch fns --------------------------------------------------------------

def fetch_indicators(node_id: str) -> None:
    """Indicator catalog: union of every archive's {P}_LABEL.csv."""
    asset = node_id
    urls = _discover_archive_urls()
    tables = []
    for name in ARCHIVES:
        zf = _open_archive(urls[name])
        member = f"{name}_LABEL.csv"
        with zf.open(member) as f:
            t = pacsv.read_csv(
                f,
                convert_options=pacsv.ConvertOptions(column_types={
                    "INDICATOR_ID": pa.string(),
                    "INDICATOR_LABEL_EN": pa.string(),
                }),
            )
        t = t.append_column(
            "DATASET", pa.array([name] * t.num_rows, pa.string())
        )
        tables.append(t.select(["INDICATOR_ID", "INDICATOR_LABEL_EN", "DATASET"]))
        print(f"  {member}: {t.num_rows} indicators", flush=True)
    combined = pa.concat_tables(tables).cast(INDICATORS_SCHEMA)
    save_raw_parquet(combined, asset)


def fetch_values(node_id: str) -> None:
    """Long-format observations: union of every archive's {P}_DATA_NATIONAL.csv.

    Streamed batch-by-batch through a single parquet writer so memory stays
    bounded regardless of corpus size (~6M rows combined).
    """
    asset = node_id
    urls = _discover_archive_urls()
    convert = pacsv.ConvertOptions(column_types={
        "INDICATOR_ID": pa.string(),
        "COUNTRY_ID": pa.string(),
        "YEAR": pa.int32(),
        "VALUE": pa.float64(),
        "MAGNITUDE": pa.string(),
        "QUALIFIER": pa.string(),
    })
    read_opts = pacsv.ReadOptions(block_size=_CSV_BLOCK_SIZE)
    total = 0
    with raw_parquet_writer(asset, VALUES_SCHEMA) as writer:
        for name in ARCHIVES:
            zf = _open_archive(urls[name])
            member = f"{name}_DATA_NATIONAL.csv"
            with zf.open(member) as f:
                reader = pacsv.open_csv(
                    f, read_options=read_opts, convert_options=convert
                )
                for batch in reader:
                    t = pa.table(batch)
                    t = t.append_column(
                        "DATASET", pa.array([name] * t.num_rows, pa.string())
                    )
                    t = t.cast(VALUES_SCHEMA)
                    writer.write_table(t)
                    total += t.num_rows
            print(f"  {member}: cumulative {total} rows", flush=True)
    if total == 0:
        raise RuntimeError(f"{asset}: parsed 0 observation rows from BDDS archives")


# --- specs ------------------------------------------------------------------

DOWNLOAD_SPECS = [
    NodeSpec(
        id="unesco-institute-for-statistics-indicators",
        fn=fetch_indicators,
        kind="download",
    ),
    NodeSpec(
        id="unesco-institute-for-statistics-values",
        fn=fetch_values,
        kind="download",
    ),
]

TRANSFORM_SPECS = [
    SqlNodeSpec(
        id="unesco-institute-for-statistics-indicators-transform",
        deps=["unesco-institute-for-statistics-indicators"],
        sql='''
            SELECT indicator_id, label, dataset
            FROM (
                SELECT
                    CAST("INDICATOR_ID" AS VARCHAR)        AS indicator_id,
                    TRIM(CAST("INDICATOR_LABEL_EN" AS VARCHAR)) AS label,
                    CAST("DATASET" AS VARCHAR)             AS dataset,
                    row_number() OVER (
                        PARTITION BY "INDICATOR_ID" ORDER BY "DATASET"
                    ) AS rn
                FROM "unesco-institute-for-statistics-indicators"
                WHERE "INDICATOR_ID" IS NOT NULL
            )
            WHERE rn = 1
        ''',
    ),
    SqlNodeSpec(
        id="unesco-institute-for-statistics-values-transform",
        deps=["unesco-institute-for-statistics-values"],
        sql='''
            SELECT
                CAST("INDICATOR_ID" AS VARCHAR) AS indicator_id,
                CAST("COUNTRY_ID" AS VARCHAR)   AS country_id,
                CAST("YEAR" AS INTEGER)         AS year,
                CAST("VALUE" AS DOUBLE)         AS value,
                NULLIF(CAST("MAGNITUDE" AS VARCHAR), '') AS magnitude,
                NULLIF(CAST("QUALIFIER" AS VARCHAR), '') AS qualifier,
                CAST("DATASET" AS VARCHAR)      AS dataset
            FROM "unesco-institute-for-statistics-values"
            WHERE "VALUE" IS NOT NULL
              AND "YEAR" IS NOT NULL
              AND "INDICATOR_ID" IS NOT NULL
              AND "COUNTRY_ID" IS NOT NULL
        ''',
    ),
]
