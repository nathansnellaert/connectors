"""Bank of England download specs.

Covers three distinct fetch surfaces, one DOWNLOAD_SPEC per collect entity:

1. ``iadb_observations`` — the canonical Interactive Statistical Database
   time-series corpus. There is no machine-readable catalog, so series codes
   are discovered by a two-level scrape: the combined A-Z page
   (CategoryIndex.asp) lists ~1600 category links; each FromShowColumns.asp
   category page lists the real series codes; observations are then pulled in
   bulk from the IADB CSV endpoint (CSVF=CT, columnar long-format, <=300
   series per request). This is a bounded *firehose*: each refresh processes a
   slice of categories under a wall-clock budget, writes one parquet batch per
   category, and advances a ``next_index`` watermark in state. A full backfill
   is a sequence of bounded refreshes; once the category list is exhausted the
   watermark resets and the corpus is re-swept (picking up revisions / adds).

2. The 9 research-dataset XLSX files (Millennium of macro data, balance-sheet
   series, QE, NMG survey, BoC/BoE macrohistory, Agents' scores, inflation
   attitudes) — one persistent XLSX URL each, fetched as opaque bytes with a
   Last-Modified conditional-GET short-circuit.

3. ``bankstats_publication_tables`` — a single persistent ZIP snapshot of all
   published Bankstats tables, fetched as opaque bytes (same conditional-GET).

Akamai bot protection 403s default User-Agents, so every request carries a
browser-like UA. The IADB endpoint and /-/media/boe/files URLs are persistent.
"""

import html as htmllib
import re
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs

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
    get,
    load_state,
    save_raw_file,
    save_raw_parquet,
    save_state,
)

# Bump when the persisted state/watermark contract changes shape.
STATE_VERSION = 1

# Akamai blocks default UAs; a browser-like UA is mandatory on every request.
# ASCII only — no smart punctuation in header values.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,text/html,application/xhtml+xml,*/*",
}

IADB_BASE = "https://www.bankofengland.co.uk/boeapps/database"
IADB_CSV_URL = f"{IADB_BASE}/_iadb-fromshowcolumns.asp"
IADB_FROMSHOWCOLUMNS_URL = f"{IADB_BASE}/FromShowColumns.asp"
IADB_CATEGORY_INDEX_URL = (
    f"{IADB_BASE}/CategoryIndex.asp"
    "?Travel=NIxAZx&CategId=allcats&CategName=Combined%20A%20to%20Z"
)

# Bulk single-file artefacts: entity_id -> (url, extension). XLSX research
# datasets + the Bankstats ZIP snapshot. URLs verified live (HEAD 200).
BULK_FILES = {
    "agents-scores": (
        "https://www.bankofengland.co.uk/-/media/boe/files/agents-summary/agentsscores.xlsx",
        "xlsx",
    ),
    "annual-boe-balance-sheet": (
        "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/annual-data-on-the-boes-balance-sheet.xlsx",
        "xlsx",
    ),
    "boc-boe-macrohistory-database": (
        "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/boc-boe-database.xlsx",
        "xlsx",
    ),
    "inflation-attitudes-survey-long-run": (
        "https://www.bankofengland.co.uk/-/media/boe/files/inflation-attitudes-survey/long-run.xlsx",
        "xlsx",
    ),
    "lender-of-last-resort-historical": (
        "https://www.bankofengland.co.uk/-/media/boe/files/research/the-bank-of-england-as-lender-of-last-resort-historical-dataset.xlsx",
        "xlsx",
    ),
    "millennium-of-macroeconomic-data-uk": (
        "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/a-millennium-of-macroeconomic-data-for-the-uk.xlsx",
        "xlsx",
    ),
    "nmg-household-survey": (
        "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/boe-nmg-household-survey-data.xlsx",
        "xlsx",
    ),
    "qe-related-data": (
        "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/qe-related-data.xlsx",
        "xlsx",
    ),
    "weekly-boe-balance-sheet-1844-2006": (
        "https://www.bankofengland.co.uk/-/media/boe/files/statistics/research-datasets/weekly-data-on-the-boes-balance-sheet-1844-to-2006.xlsx",
        "xlsx",
    ),
    "bankstats_publication_tables": (
        "https://www.bankofengland.co.uk/-/media/boe/files/statistics/bankstats-latest-tables.zip",
        "zip",
    ),
}

# Long-format IADB observations. Declared once; every batch conforms.
IADB_SCHEMA = pa.schema(
    [
        ("series_code", pa.string()),
        ("date", pa.date32()),
        ("value", pa.float64()),
    ]
)

# Earliest IADB data per docs; "now" lets the endpoint cap at the latest obs.
IADB_DATE_FROM = "01/Jan/1963"
IADB_SERIES_PER_REQUEST = 300

# Bounded-firehose pacing for the IADB sweep. Soft caps: hitting them returns
# cleanly with the watermark advanced; the next refresh resumes.
MAX_FETCH_SECONDS = 300
# Hard ceiling that RAISES — detects the catalog ballooning past any plausible
# size (structure change), rather than silently truncating. ~1600 today.
MAX_CATEGORIES = 20000


# --------------------------------------------------------------------------- #
# Transport: retry on transient failures only.
# --------------------------------------------------------------------------- #
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
def _get(url: str, *, params=None, headers=None, timeout=(10.0, 120.0)):
    """Retried GET. 4xx (except 429) and 304 surface to the caller; transient
    failures are retried with backoff and reraised once exhausted."""
    merged = dict(BROWSER_HEADERS)
    if headers:
        merged.update(headers)
    resp = get(url, params=params, headers=merged, timeout=timeout)
    # 304 is a successful conditional response, not an error — don't raise.
    if resp.status_code != 304:
        resp.raise_for_status()
    return resp


# --------------------------------------------------------------------------- #
# Bulk single-file artefacts (XLSX research datasets + Bankstats ZIP).
# --------------------------------------------------------------------------- #
def fetch_bulk_file(entity_id: str) -> None:
    asset = f"bank-of-england-{entity_id.lower().replace('_', '-')}"
    url, ext = BULK_FILES[entity_id]

    state = load_state(asset)
    if state.get("schema_version") != STATE_VERSION:
        state = {}

    headers = {}
    last_modified = state.get("last_modified")
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    try:
        resp = _get(url, headers=headers, timeout=(10.0, 300.0))
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        # Permanent (moved/withdrawn). Record a TTL-bound skip and move on.
        print(f"[bank-of-england] {asset}: permanent {code} for {url}")
        skipped = state.get("skipped", {})
        skipped[asset] = {
            "reason": f"HTTP {code}",
            "expires_at": int(time.time()) + 14 * 86400,
        }
        state.update({"schema_version": STATE_VERSION, "skipped": skipped})
        save_state(asset, state)
        return

    if resp.status_code == 304:
        print(f"[bank-of-england] {asset}: unchanged (304), skipping")
        return

    content = resp.content
    save_raw_file(content, asset, extension=ext)  # raw FIRST
    save_state(
        asset,
        {
            "schema_version": STATE_VERSION,
            "last_modified": resp.headers.get("last-modified"),
            "last_run_stats": {
                "bytes": len(content),
                "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
            },
        },
    )


# --------------------------------------------------------------------------- #
# IADB observations — bounded firehose over the scraped category catalog.
# --------------------------------------------------------------------------- #
def _discover_categories() -> list[dict]:
    """Scrape the combined A-Z page into an ordered, de-duplicated list of
    category descriptors. Each descriptor carries the params FromShowColumns
    needs (NewMeaningId + CategId + HighlightCatValueDisplay are all required —
    omitting the highlight value yields an empty page)."""
    resp = _get(IADB_CATEGORY_INDEX_URL, timeout=(10.0, 120.0))
    html_text = resp.text
    hrefs = re.findall(r'href="(FromShowColumns\.asp\?[^"]+)"', html_text)

    cats: list[dict] = []
    seen = set()
    for href in hrefs:
        qs = htmllib.unescape(href).split("?", 1)[1]
        params = parse_qs(qs, keep_blank_values=True)
        nm = params.get("NewMeaningId", [""])[0]
        if not nm:
            continue
        cid = params.get("CategId", [""])[0]
        hl = params.get("HighlightCatValueDisplay", [""])[0]
        key = (nm, cid, hl)
        if key in seen:
            continue
        seen.add(key)
        cats.append(
            {"NewMeaningId": nm, "CategId": cid, "HighlightCatValueDisplay": hl}
        )

    if not cats:
        raise RuntimeError(
            "IADB catalog scrape found 0 categories — A-Z page layout changed"
        )
    if len(cats) > MAX_CATEGORIES:
        raise RuntimeError(
            f"IADB catalog returned {len(cats)} categories (> {MAX_CATEGORIES}) "
            "— structure change, refusing to sweep blindly"
        )
    return cats


def _category_series_codes(cat: dict) -> list[str]:
    """Fetch one FromShowColumns category page and extract the real series
    codes (deduplicated, order-preserved)."""
    params = {"FromCategoryList": "Yes", **cat}
    resp = _get(IADB_FROMSHOWCOLUMNS_URL, params=params, timeout=(10.0, 120.0))
    codes: list[str] = []
    seen = set()
    for code in re.findall(r"SeriesCodes=([A-Za-z0-9]+)", resp.text):
        if code not in seen:
            seen.add(code)
            codes.append(code)
    return codes


def _parse_iadb_ct(text: str) -> list[dict]:
    """Parse CSVF=CT output. Two blocks: a 'SERIES,DESCRIPTION' header block
    (descriptions contain commas — skipped) then a 'DATE,SERIES,VALUE' block of
    one observation per line."""
    rows: list[dict] = []
    mode = None
    for ln in text.splitlines():
        if ln.startswith("DATE,SERIES,VALUE"):
            mode = "obs"
            continue
        if ln.startswith("SERIES,DESCRIPTION"):
            mode = "desc"
            continue
        if mode != "obs":
            continue
        parts = ln.split(",")
        if len(parts) < 3 or not parts[0]:
            continue
        try:
            d = datetime.strptime(parts[0].strip(), "%d %b %Y").date()
        except ValueError:
            continue  # unrecognised date rendering — drop the obs
        raw_val = parts[2].strip()
        try:
            v = float(raw_val) if raw_val else None
        except ValueError:
            v = None
        rows.append({"series_code": parts[1], "date": d, "value": v})
    return rows


def _fetch_observations(codes: list[str]) -> list[dict]:
    """Pull observations for a code list via the IADB CSV endpoint, batching at
    the 300-series request limit."""
    rows: list[dict] = []
    for i in range(0, len(codes), IADB_SERIES_PER_REQUEST):
        chunk = codes[i : i + IADB_SERIES_PER_REQUEST]
        params = {
            "csv.x": "yes",
            "Datefrom": IADB_DATE_FROM,
            "Dateto": "now",
            "SeriesCodes": ",".join(chunk),
            "CSVF": "CT",
            "UsingCodes": "Y",
            "VPD": "Y",
            "VFD": "N",
        }
        resp = _get(IADB_CSV_URL, params=params, timeout=(10.0, 180.0))
        text = resp.text
        # Invalid code-lists render an HTML error page rather than CSV.
        if text.lstrip()[:9].lower().startswith("<!doctype") or "SERIES,DESCRIPTION" not in text:
            continue
        rows.extend(_parse_iadb_ct(text))
    return rows


def fetch_iadb_observations(entity_id: str) -> None:
    state_key = f"bank-of-england-{entity_id.lower().replace('_', '-')}"
    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        state = {}

    cats = state.get("categories")
    next_index = state.get("next_index", 0)

    # Fresh start, or one full sweep completed -> re-discover and re-sweep
    # (picks up new series + revisions). Persist the catalog before crawling.
    if not cats or next_index >= len(cats):
        cats = _discover_categories()
        next_index = 0
        state = {
            "schema_version": STATE_VERSION,
            "categories": cats,
            "next_index": 0,
        }
        save_state(state_key, state)
        print(f"[bank-of-england] iadb: discovered {len(cats)} categories")

    deadline = time.monotonic() + MAX_FETCH_SECONDS
    processed = 0
    rows_written = 0
    idx = next_index

    while idx < len(cats):
        if time.monotonic() > deadline:
            break
        cat = cats[idx]
        try:
            codes = _category_series_codes(cat)
            if codes:
                rows = _fetch_observations(codes)
                if rows:
                    table = pa.Table.from_pylist(rows, schema=IADB_SCHEMA)
                    save_raw_parquet(table, f"{state_key}-{idx:05d}")  # raw FIRST
                    rows_written += len(rows)
        except httpx.HTTPStatusError as exc:
            # Permanent per-category failure (4xx other than 429, which is
            # transient and already retried). Skip the category, keep sweeping.
            print(
                f"[bank-of-england] iadb cat {idx} "
                f"({cat.get('NewMeaningId', '')[:20]}): "
                f"HTTP {exc.response.status_code}, skipping"
            )

        idx += 1
        processed += 1
        save_state(  # advance watermark AFTER raw is durable
            state_key,
            {
                "schema_version": STATE_VERSION,
                "categories": cats,
                "next_index": idx,
                "last_run_stats": {
                    "categories_this_run": processed,
                    "rows_this_run": rows_written,
                    "swept_at": datetime.now(tz=timezone.utc).isoformat(),
                },
            },
        )
        if processed % 10 == 0:
            print(
                f"[bank-of-england] iadb: {idx}/{len(cats)} categories, "
                f"{rows_written:,} rows this run",
                flush=True,
            )

    print(
        f"[bank-of-england] iadb run done: {processed} categories, "
        f"{rows_written:,} rows, watermark {idx}/{len(cats)}",
        flush=True,
    )


# --------------------------------------------------------------------------- #
# DOWNLOAD_SPECS — one per entity-union entry.
# --------------------------------------------------------------------------- #
DOWNLOAD_SPECS = [
    NodeSpec(
        id="bank-of-england-iadb-observations",
        fn=fetch_iadb_observations,
        args=("iadb_observations",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-agents-scores",
        fn=fetch_bulk_file,
        args=("agents-scores",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-annual-boe-balance-sheet",
        fn=fetch_bulk_file,
        args=("annual-boe-balance-sheet",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-bankstats-publication-tables",
        fn=fetch_bulk_file,
        args=("bankstats_publication_tables",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-boc-boe-macrohistory-database",
        fn=fetch_bulk_file,
        args=("boc-boe-macrohistory-database",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-inflation-attitudes-survey-long-run",
        fn=fetch_bulk_file,
        args=("inflation-attitudes-survey-long-run",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-lender-of-last-resort-historical",
        fn=fetch_bulk_file,
        args=("lender-of-last-resort-historical",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-millennium-of-macroeconomic-data-uk",
        fn=fetch_bulk_file,
        args=("millennium-of-macroeconomic-data-uk",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-nmg-household-survey",
        fn=fetch_bulk_file,
        args=("nmg-household-survey",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-qe-related-data",
        fn=fetch_bulk_file,
        args=("qe-related-data",),
        deps=(),
        kind="download",
    ),
    NodeSpec(
        id="bank-of-england-weekly-boe-balance-sheet-1844-2006",
        fn=fetch_bulk_file,
        args=("weekly-boe-balance-sheet-1844-2006",),
        deps=(),
        kind="download",
    ),
]
