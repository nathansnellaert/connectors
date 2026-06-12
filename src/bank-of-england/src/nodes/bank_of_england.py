"""Bank of England connector — IADB statistics + research-dataset workbooks.

Three fetch shapes, all stateless full re-pulls (every entity is small enough
to re-fetch whole each run; revisions and late corrections are picked up for
free because no watermark is trusted):

  - iadb_observations          : the IADB CSV endpoint (chosen mechanism).
                                 A curated "hot subset" of ~well-known series
                                 codes (SONIA, Bank Rate, M4, FX, gilt yields)
                                 fetched as long-format observations. Full
                                 corpus discovery needs HTML catalog scraping
                                 (out of scope here); the codes below were
                                 validated live against the endpoint on
                                 2026-06-12 — invalid codes error the whole
                                 batch, so only confirmed-live codes are used.
  - bankstats_publication_tables : the monthly bulk ZIP of published Bankstats
                                 tables (.xls workbooks).
  - 9 research-dataset workbooks : one persistent XLSX URL per dataset
                                 (Millennium of UK macro data, BoE balance
                                 sheets, QE, NMG survey, BoC/BoE macrohistory,
                                 Agents' scores, Inflation attitudes survey).

The research/bankstats workbooks are wildly heterogeneous — metadata sheets,
multi-row headers, some transposed (dates across columns). Rather than guess a
per-file tidy schema, the fetch fns emit a lossless cell-grid long format
(sheet, row, col, value) — structural surgery beyond what SQL can express is
done here in Python; the SQL transform stays a thin parse-and-type pass.

Akamai bot protection 403s default User-Agents, so every fetch fn sets a
browser-like UA via configure_http before issuing requests.
"""
import io
import zipfile
from datetime import datetime

import httpx
import numpy as np
import pandas as pd
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
    configure_http,
    get,
    raw_parquet_writer,
    save_raw_parquet,
)

# Browser-like UA (ASCII only) — required to clear the Akamai bot check.
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

_IADB_URL = (
    "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
)
_BANKSTATS_ZIP_URL = (
    "https://www.bankofengland.co.uk/-/media/boe/files/statistics/"
    "bankstats-latest-tables.zip"
)

# Persistent XLSX URL per research dataset (keys = stripped spec ids).
_RESEARCH_XLSX = {
    "agents-scores": "https://www.bankofengland.co.uk/-/media/boe/files/agents-summary/agentsscores.xlsx",
    "annual-boe-balance-sheet": "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/annual-data-on-the-boes-balance-sheet.xlsx",
    "boc-boe-macrohistory-database": "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/boc-boe-database.xlsx",
    "inflation-attitudes-survey-long-run": "https://www.bankofengland.co.uk/-/media/boe/files/inflation-attitudes-survey/long-run.xlsx",
    "lender-of-last-resort-historical": "https://www.bankofengland.co.uk/-/media/boe/files/research/the-bank-of-england-as-lender-of-last-resort-historical-dataset.xlsx",
    "millennium-of-macroeconomic-data-uk": "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/a-millennium-of-macroeconomic-data-for-the-uk.xlsx",
    "nmg-household-survey": "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/boe-nmg-household-survey-data.xlsx",
    "qe-related-data": "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/qe-related-data.xlsx",
    "weekly-boe-balance-sheet-1844-2006": "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/weekly-data-on-the-boes-balance-sheet-1844-to-2006.xlsx",
}

# Curated IADB "hot subset" — each code validated live (returns rows) on
# 2026-06-12. Well under the 300-codes-per-request cap, so one request fetches
# the lot. Discontinued / zero-row codes were dropped during validation.
IADB_SERIES_CODES = [
    "IUDBEDR",   # Official Bank Rate (daily)
    "IUDSOIA",   # SONIA (daily)
    "IUMABEDR",  # Monthly average official Bank Rate
    "XUDLUSS",   # Spot USD into Sterling
    "XUDLERS",   # Spot EUR into Sterling
    "XUDLSER",   # Spot Sterling into EUR
    "XUDLGBD",   # Spot Sterling into USD
    "XUDLJYS",   # Spot JPY into Sterling
    "XUDLERD",   # Spot EUR into USD
    "XUDLB8KL",  # Spot BRL into USD
    "XUDLBK67",  # Sterling effective exchange rate index
    "LPMVWYR",   # M4 lending, amounts outstanding (monthly)
    "LPMAUYN",   # M4, amounts outstanding (monthly)
    "LPMAVAA",   # Total sterling, average amount outstanding (monthly)
    "LPMAVAB",   # Total sterling, average amount outstanding (monthly)
    "LPMVQJW",   # M4 12-month growth rate (monthly)
    "LPMAUZI",   # M4 changes (monthly)
    "LPMVWYL",   # MFI sterling changes (monthly)
    "LPMBC57",   # MFI amounts outstanding (monthly)
    "LPQAUYN",   # M4, amounts outstanding (quarterly)
    "RPMTBVE",   # UK resident MFI amounts outstanding (monthly)
    "IUDMNZC",   # 10-year nominal gilt yield (daily)
    "IUDSNPY",   # 5-year nominal gilt yield (daily)
]

# Long-format observation schema for the IADB endpoint.
_SCHEMA_IADB = pa.schema([
    ("date", pa.string()),          # ISO YYYY-MM-DD
    ("series_code", pa.string()),
    ("value", pa.float64()),        # nullable — non-numeric cells -> null
])

# Lossless cell-grid schema for research workbooks.
_SCHEMA_CELL = pa.schema([
    ("sheet", pa.string()),
    ("row", pa.int64()),
    ("col", pa.int64()),
    ("value", pa.string()),
])

# Cell-grid for the Bankstats ZIP carries the source table filename too.
_SCHEMA_BANKSTATS = pa.schema([
    ("table_file", pa.string()),
    ("sheet", pa.string()),
    ("row", pa.int64()),
    ("col", pa.int64()),
    ("value", pa.string()),
])

_IADB_ID = "bank-of-england-iadb-observations"
_BANKSTATS_ID = "bank-of-england-bankstats-publication-tables"
_PREFIX = "bank-of-england-"


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
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


def _ensure_ua() -> None:
    """Install the browser-like UA for this subprocess's HTTP client."""
    configure_http(headers={"User-Agent": _UA})


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch(url: str, params: dict | None = None) -> httpx.Response:
    resp = get(url, params=params, timeout=(10.0, 180.0))
    resp.raise_for_status()
    return resp


# --------------------------------------------------------------------------- #
# Workbook -> cell-grid helper
# --------------------------------------------------------------------------- #
def _sheet_to_batch(df, sheet, schema, table_file=None):
    """Melt one spreadsheet sheet (read header=None) into a cell-grid batch.

    Lossless: one row per non-empty cell, value stringified. Returns None for
    empty sheets.
    """
    if df is None or df.empty:
        return None
    arr = df.to_numpy(dtype=object)
    mask = ~pd.isna(arr)
    rs, cs = np.nonzero(mask)
    if len(rs) == 0:
        return None
    vals = [str(arr[r, c]) for r, c in zip(rs, cs)]
    n = len(vals)
    data = {
        "sheet": pa.array([sheet] * n, pa.string()),
        "row": pa.array(rs.tolist(), pa.int64()),
        "col": pa.array(cs.tolist(), pa.int64()),
        "value": pa.array(vals, pa.string()),
    }
    if table_file is not None:
        data["table_file"] = pa.array([table_file] * n, pa.string())
    return pa.record_batch([data[name] for name in schema.names], schema=schema)


# --------------------------------------------------------------------------- #
# Fetch functions
# --------------------------------------------------------------------------- #
def fetch_iadb(node_id: str) -> None:
    """Fetch the curated IADB series as long-format observations (full history)."""
    _ensure_ua()
    params = {
        "csv.x": "yes",
        "Datefrom": "01/Jan/1963",   # IADB corpus start; per-series actual start varies
        "Dateto": "now",
        "SeriesCodes": ",".join(IADB_SERIES_CODES),
        "CSVF": "CT",                # Columnar with titles -> clean long format
        "UsingCodes": "Y",
        "VPD": "Y",
        "VFD": "N",
    }
    resp = _fetch(_IADB_URL, params=params)
    text = resp.text
    if not text.lstrip().startswith("SERIES"):
        # Akamai block or endpoint change — surface loudly rather than persist garbage.
        raise RuntimeError(
            f"IADB endpoint returned a non-CSV payload (first 200 chars): {text[:200]!r}"
        )

    lines = text.splitlines()
    start = None
    for i, ln in enumerate(lines):
        if ln.strip().startswith("DATE,SERIES,VALUE"):
            start = i + 1
            break
    if start is None:
        raise RuntimeError("IADB CSV: no 'DATE,SERIES,VALUE' data header found")

    dates: list[str] = []
    codes: list[str] = []
    values: list[float | None] = []
    for ln in lines[start:]:
        ln = ln.strip()
        if not ln:
            continue
        parts = ln.split(",")
        if len(parts) < 3:
            continue
        d, code, val = parts[0], parts[1], parts[2]
        try:
            iso = datetime.strptime(d, "%d %b %Y").strftime("%Y-%m-%d")
        except ValueError:
            continue
        try:
            v: float | None = float(val)
        except ValueError:
            v = None
        dates.append(iso)
        codes.append(code)
        values.append(v)

    if not dates:
        raise RuntimeError("IADB CSV: parsed 0 observations")

    table = pa.table(
        {
            "date": pa.array(dates, pa.string()),
            "series_code": pa.array(codes, pa.string()),
            "value": pa.array(values, pa.float64()),
        },
        schema=_SCHEMA_IADB,
    )
    save_raw_parquet(table, node_id)
    print(
        f"  iadb: {len(dates)} observations across {len(set(codes))} series",
        flush=True,
    )


def fetch_research_xlsx(node_id: str) -> None:
    """Download one research-dataset XLSX and emit its lossless cell grid."""
    _ensure_ua()
    entity = node_id[len(_PREFIX):]
    url = _RESEARCH_XLSX[entity]
    resp = _fetch(url)

    xf = pd.ExcelFile(io.BytesIO(resp.content))
    wrote = False
    with raw_parquet_writer(node_id, _SCHEMA_CELL) as w:
        for sheet in xf.sheet_names:
            batch = _sheet_to_batch(
                xf.parse(sheet, header=None), sheet, _SCHEMA_CELL
            )
            if batch is None:
                continue
            w.write_batch(batch)
            wrote = True
    if not wrote:
        raise RuntimeError(f"{node_id}: extracted 0 cells from {url}")
    print(f"  {entity}: cells written from {len(xf.sheet_names)} sheets", flush=True)


def fetch_bankstats(node_id: str) -> None:
    """Download the Bankstats bulk ZIP and emit a cell grid per .xls table."""
    _ensure_ua()
    resp = _fetch(_BANKSTATS_ZIP_URL)
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    members = [
        n for n in zf.namelist() if n.lower().endswith((".xls", ".xlsx"))
    ]
    if not members:
        raise RuntimeError("bankstats ZIP: no .xls/.xlsx members found")

    wrote = False
    with raw_parquet_writer(node_id, _SCHEMA_BANKSTATS) as w:
        for member in members:
            table_file = member.rsplit("/", 1)[-1]
            try:
                xf = pd.ExcelFile(io.BytesIO(zf.read(member)))
            except Exception as exc:  # noqa: BLE001 - one bad workbook must not sink the asset
                print(
                    f"  skip bankstats member {member}: "
                    f"{type(exc).__name__}: {exc}",
                    flush=True,
                )
                continue
            for sheet in xf.sheet_names:
                batch = _sheet_to_batch(
                    xf.parse(sheet, header=None),
                    sheet,
                    _SCHEMA_BANKSTATS,
                    table_file=table_file,
                )
                if batch is None:
                    continue
                w.write_batch(batch)
                wrote = True
    if not wrote:
        raise RuntimeError(f"{node_id}: extracted 0 cells from Bankstats ZIP")
    print(f"  bankstats: cells written from {len(members)} tables", flush=True)


# --------------------------------------------------------------------------- #
# Specs
# --------------------------------------------------------------------------- #
DOWNLOAD_SPECS = [
    NodeSpec(id="bank-of-england-agents-scores", fn=fetch_research_xlsx, kind="download"),
    NodeSpec(id="bank-of-england-annual-boe-balance-sheet", fn=fetch_research_xlsx, kind="download"),
    NodeSpec(id="bank-of-england-bankstats-publication-tables", fn=fetch_bankstats, kind="download"),
    NodeSpec(id="bank-of-england-boc-boe-macrohistory-database", fn=fetch_research_xlsx, kind="download"),
    NodeSpec(id="bank-of-england-iadb-observations", fn=fetch_iadb, kind="download"),
    NodeSpec(id="bank-of-england-inflation-attitudes-survey-long-run", fn=fetch_research_xlsx, kind="download"),
    NodeSpec(id="bank-of-england-lender-of-last-resort-historical", fn=fetch_research_xlsx, kind="download"),
    NodeSpec(id="bank-of-england-millennium-of-macroeconomic-data-uk", fn=fetch_research_xlsx, kind="download"),
    NodeSpec(id="bank-of-england-nmg-household-survey", fn=fetch_research_xlsx, kind="download"),
    NodeSpec(id="bank-of-england-qe-related-data", fn=fetch_research_xlsx, kind="download"),
    NodeSpec(id="bank-of-england-weekly-boe-balance-sheet-1844-2006", fn=fetch_research_xlsx, kind="download"),
]


def _cell_transform_sql(dep: str, with_table_file: bool) -> str:
    tf = '"table_file",\n            ' if with_table_file else ""
    return f'''
        SELECT
            {tf}sheet,
            CAST("row" AS BIGINT) AS row_index,
            CAST("col" AS BIGINT) AS col_index,
            value,
            TRY_CAST(value AS DOUBLE) AS value_numeric
        FROM "{dep}"
        WHERE value IS NOT NULL
    '''


def _iadb_transform_sql(dep: str) -> str:
    return f'''
        SELECT
            CAST(date AS DATE) AS date,
            series_code,
            CAST(value AS DOUBLE) AS value
        FROM "{dep}"
        WHERE value IS NOT NULL
    '''


def _build_transform_specs() -> list[SqlNodeSpec]:
    specs = []
    for s in DOWNLOAD_SPECS:
        if s.id == _IADB_ID:
            sql = _iadb_transform_sql(s.id)
        elif s.id == _BANKSTATS_ID:
            sql = _cell_transform_sql(s.id, with_table_file=True)
        else:
            sql = _cell_transform_sql(s.id, with_table_file=False)
        specs.append(SqlNodeSpec(id=f"{s.id}-transform", deps=[s.id], sql=sql))
    return specs


TRANSFORM_SPECS = _build_transform_specs()
