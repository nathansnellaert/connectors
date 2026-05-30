"""Bank of England download specs.

Three fetch shapes across 11 collect entities:

1. Static research files (9 XLSX + 1 ZIP) — each a single persistent
   `/-/media/boe/files/...` URL holding a full historical compilation. Opaque
   bytes, fetched whole and written via `save_raw_file`; the transform step
   parses the workbook/zip. `fetch_static_file`.

2. IADB observations corpus (`iadb_observations`) — the canonical Interactive
   Statistical Database, ~10,000 series across the published A-Z catalog. There
   is NO machine-readable series catalog, so series codes are discovered by
   scraping the static HTML catalog (CategoryIndex.asp combined A-Z page →
   per-category FromShowColumns.asp listings, session-cookie gated), then their
   observations are pulled from the CSV endpoint (CSVF=CT long format, up to a
   few hundred codes per request). Because full discovery requires walking
   ~1700 catalog category pages, this is a batched / firehose-shaped fetch
   (`fetch_iadb_observations`): each refresh processes categories up to a
   wall-clock budget, writes per-chunk parquet batches, and resumes from a
   category cursor stored in state. A cycle ends when every category has been
   walked; the next cycle restarts from cursor 0 to pick up revisions. Series
   are deduplicated across categories via a `seen` set so each series'
   observations are fetched once per cycle; transform globs the batch files.

   Cost note: the catalog walk dominates (one HTML request per category, vs a
   handful of CSV data requests) because the catalog is HTML-only. There is no
   cheaper complete enumeration of the IADB corpus.

Auth: none, but Akamai bot protection 403s default User-Agents, so every fetch
fn sets a browser-like UA via `configure_http`.
"""
import hashlib
import html
import re
import time
from urllib.parse import parse_qsl

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
    configure_http,
    get,
    load_state,
    save_raw_file,
    save_raw_parquet,
    save_state,
)

STATE_VERSION = 1

# Browser-like UA — Akamai bot protection 403s the default. ASCII only.
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# --- Static research files: spec id -> (path under media base, extension) ---
RESEARCH_BASE = "https://www.bankofengland.co.uk/-/media/boe/files/"
STATIC_FILES = {
    "bank-of-england-agents-scores": (
        "agents-summary/agentsscores.xlsx", "xlsx"),
    "bank-of-england-annual-boe-balance-sheet": (
        "statistics/research-datasets/annual-data-on-the-boes-balance-sheet.xlsx", "xlsx"),
    "bank-of-england-boc-boe-macrohistory-database": (
        "statistics/research-datasets/boc-boe-database.xlsx", "xlsx"),
    "bank-of-england-inflation-attitudes-survey-long-run": (
        "inflation-attitudes-survey/long-run.xlsx", "xlsx"),
    "bank-of-england-lender-of-last-resort-historical": (
        "research/the-bank-of-england-as-lender-of-last-resort-historical-dataset.xlsx", "xlsx"),
    "bank-of-england-millennium-of-macroeconomic-data-uk": (
        "statistics/research-datasets/a-millennium-of-macroeconomic-data-for-the-uk.xlsx", "xlsx"),
    "bank-of-england-nmg-household-survey": (
        "statistics/research-datasets/boe-nmg-household-survey-data.xlsx", "xlsx"),
    "bank-of-england-qe-related-data": (
        "statistics/research-datasets/qe-related-data.xlsx", "xlsx"),
    "bank-of-england-weekly-boe-balance-sheet-1844-2006": (
        "statistics/research-datasets/weekly-data-on-the-boes-balance-sheet-1844-to-2006.xlsx", "xlsx"),
    "bank-of-england-bankstats-publication-tables": (
        "statistics/bankstats-latest-tables.zip", "zip"),
}

# --- IADB endpoints ---
IADB_DATA_URL = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
CATALOG_AZ_URL = "https://www.bankofengland.co.uk/boeapps/database/CategoryIndex.asp"
FROMSHOWCOLUMNS_URL = "https://www.bankofengland.co.uk/boeapps/database/FromShowColumns.asp"
CATALOG_AZ_PARAMS = {"Travel": "NIxAZx", "CategId": "allcats", "CategName": "Combined A to Z"}

# IADB series begin no earlier than 1963; the endpoint rejects earlier Datefrom
# with an HTML error. Each series returns only its own available range.
IADB_DATE_FROM = "01/Jan/1963"
SERIES_PER_REQUEST = 100         # bounds batch size / memory; endpoint allows 300
CATALOG_PAGE_SIZE = 150          # FromShowColumns results per page
MAX_PAGES_PER_CATEGORY = 100     # safety ceiling (15k series/category is absurd)
MAX_FETCH_SECONDS = 1500         # soft per-run budget (~25 min); resume next run

IADB_SPEC_ID = "bank-of-england-iadb-observations"

# Raw observation schema (long format). Values kept as strings in the raw layer
# to losslessly preserve blanks / provisional markers; transform coerces.
IADB_SCHEMA = pa.schema([
    ("series_code", pa.string()),
    ("description", pa.string()),
    ("date", pa.string()),
    ("value", pa.string()),
])


# --------------------------------------------------------------------------
# Transport: retried request with honest transient classification.
# --------------------------------------------------------------------------
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
def _request(url: str, params: dict | None = None) -> httpx.Response:
    resp = get(url, params=params, timeout=(15.0, 180.0))
    resp.raise_for_status()
    return resp


# --------------------------------------------------------------------------
# Static research file downloads (XLSX / ZIP).
# --------------------------------------------------------------------------
def fetch_static_file(node_id: str) -> None:
    configure_http(headers={"User-Agent": BROWSER_UA})
    path, ext = STATIC_FILES[node_id]
    url = RESEARCH_BASE + path
    resp = _request(url)
    content = resp.content
    # XLSX and ZIP are both PK-zip containers; a truncated/HTML response would
    # fail this and surface loudly rather than persisting garbage.
    if not content or len(content) < 1024 or content[:2] != b"PK":
        raise AssertionError(
            f"{node_id}: unexpected payload from {url} "
            f"({len(content) if content else 0} bytes, head={content[:8]!r})"
        )
    save_raw_file(content, node_id, extension=ext)


# --------------------------------------------------------------------------
# IADB catalog discovery helpers.
# --------------------------------------------------------------------------
def _refresh_session() -> httpx.Response:
    """GET the combined A-Z page; sets the ASP session cookie on the shared
    client (required for FromShowColumns) and returns the page."""
    return _request(CATALOG_AZ_URL, params=CATALOG_AZ_PARAMS)


def _parse_categories(text: str) -> list[dict]:
    """Extract the per-category FromShowColumns query params from the A-Z page.

    Each catalog category value links to FromShowColumns.asp with the params
    (NewMeaningId, CategId, HighlightCatValueDisplay, ...) needed to list that
    category's series. Order is document order — deterministic across runs."""
    cats: list[dict] = []
    seen_keys: set[tuple] = set()
    for m in re.finditer(r'href="([^"]*FromShowColumns\.asp\?[^"]+)"', text):
        href = html.unescape(m.group(1))
        query = href.split("?", 1)[1]
        params = dict(parse_qsl(query, keep_blank_values=True))
        meaning = params.get("NewMeaningId")
        if not meaning:
            continue
        key = (params.get("CategId", ""), meaning,
               params.get("HighlightCatValueDisplay", ""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        cats.append(params)
    return cats


def _cat_fingerprint(cats: list[dict]) -> str:
    h = hashlib.sha256()
    for c in cats:
        token = "|".join((
            c.get("CategId", ""),
            c.get("NewMeaningId", ""),
            c.get("HighlightCatValueDisplay", ""),
        ))
        h.update(token.encode("utf-8", "replace"))
    return h.hexdigest()


def _series_codes_for_category(cat_params: dict, _session_retried: bool = False) -> list[str]:
    """Page through a category's FromShowColumns listing, collecting series codes."""
    codes: list[str] = []
    total: int | None = None
    page = 1
    while True:
        params = dict(cat_params)
        if page > 1:
            params["ShadowPage"] = str(page)
        text = _request(FROMSHOWCOLUMNS_URL, params=params).text
        if "<title>Error</title>" in text:
            # Likely an expired session cookie — refresh once and retry.
            if not _session_retried:
                _refresh_session()
                return _series_codes_for_category(cat_params, _session_retried=True)
            print(f"[iadb] category errored after session refresh: "
                  f"{cat_params.get('HighlightCatValueDisplay', '?')[:40]}", flush=True)
            return list(dict.fromkeys(codes))
        page_codes = re.findall(r"SeriesCodes=([A-Z0-9]+)", text)
        codes.extend(page_codes)
        if total is None:
            mt = re.search(r'name="TotalNumResults"\s+VALUE="(\d+)"', text)
            total = int(mt.group(1)) if mt else len(page_codes)
        if not page_codes or page * CATALOG_PAGE_SIZE >= total:
            break
        page += 1
        if page > MAX_PAGES_PER_CATEGORY:
            raise RuntimeError(
                f"category {cat_params.get('NewMeaningId')} exceeded "
                f"{MAX_PAGES_PER_CATEGORY} pages (total={total})"
            )
    return list(dict.fromkeys(codes))


def _parse_iadb_csv(text: str) -> dict:
    """Parse CSVF=CT (columnar-with-titles) into column arrays.

    Layout: 'SERIES,DESCRIPTION' header + one row per series, a blank line,
    then 'DATE,SERIES,VALUE' and long-format observation rows."""
    lines = text.splitlines()
    descriptions: dict[str, str] = {}
    for ln in lines[1:]:
        if not ln.strip() or ln.startswith("DATE,SERIES,VALUE"):
            break
        code, _, desc = ln.partition(",")  # description may contain commas
        descriptions[code.strip()] = desc.strip()

    data_start = None
    for idx, ln in enumerate(lines):
        if ln.startswith("DATE,SERIES,VALUE"):
            data_start = idx
            break

    s_code: list[str] = []
    s_desc: list[str] = []
    s_date: list[str] = []
    s_val: list[str] = []
    if data_start is not None:
        for ln in lines[data_start + 1:]:
            if not ln.strip():
                continue
            parts = ln.split(",")
            if len(parts) < 3:
                continue
            date, code, value = parts[0].strip(), parts[1].strip(), parts[2].strip()
            s_code.append(code)
            s_desc.append(descriptions.get(code, ""))
            s_date.append(date)
            s_val.append(value)
    return {"series_code": s_code, "description": s_desc, "date": s_date, "value": s_val}


def _fetch_observations(codes: list[str]) -> dict | None:
    """Fetch CSV observations for a chunk of series codes. Returns column arrays,
    or None if the endpoint returned a (non-CSV) error page for the whole chunk."""
    text = _request(IADB_DATA_URL, params={
        "csv.x": "yes",
        "Datefrom": IADB_DATE_FROM,
        "Dateto": "now",
        "SeriesCodes": ",".join(codes),
        "CSVF": "CT",
        "UsingCodes": "Y",
        "VPD": "Y",
        "VFD": "N",
    }).text
    if not text.lstrip().startswith("SERIES,"):
        return None
    return _parse_iadb_csv(text)


def _chunks(seq: list, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# --------------------------------------------------------------------------
# IADB observations — batched / resumable corpus fetch.
# --------------------------------------------------------------------------
def fetch_iadb_observations(node_id: str) -> None:
    configure_http(headers={"User-Agent": BROWSER_UA})
    deadline = time.monotonic() + MAX_FETCH_SECONDS

    state = load_state(node_id)
    if state.get("schema_version") != STATE_VERSION:
        state = {}

    # Always refresh the session cookie and re-derive the category list (one
    # cheap request). Category order is stable; a fingerprint mismatch means the
    # catalog changed under us, so we restart the cycle to stay consistent.
    categories = _parse_categories(_refresh_session().text)
    if not categories:
        raise RuntimeError("IADB A-Z catalog returned no categories")
    fingerprint = _cat_fingerprint(categories)

    cursor = state.get("cursor", 0)
    seen = set(state.get("seen", []))
    cycle = state.get("cycle", 0)
    if state.get("cat_fingerprint") != fingerprint or cursor >= len(categories):
        cursor = 0
        seen = set()
        cycle += 1

    rows_this_run = 0
    batches_this_run = 0
    while cursor < len(categories):
        if time.monotonic() > deadline:
            print(f"[iadb] budget reached at category {cursor}/{len(categories)}; "
                  f"{len(seen)} series seen so far", flush=True)
            break

        cat = categories[cursor]
        try:
            codes = _series_codes_for_category(cat)
        except httpx.HTTPStatusError as exc:
            # Permanent HTTP error on one category: log, skip, keep going.
            print(f"[iadb] category {cursor} HTTP {exc.response.status_code} "
                  f"({cat.get('HighlightCatValueDisplay', '?')[:40]}); skipping", flush=True)
            codes = []

        new_codes = [c for c in codes if c not in seen]
        for j, chunk in enumerate(_chunks(new_codes, SERIES_PER_REQUEST)):
            cols = _fetch_observations(chunk)
            if not cols or not cols["series_code"]:
                if cols is None:
                    print(f"[iadb] no CSV for chunk in category {cursor} "
                          f"({len(chunk)} codes); skipping", flush=True)
                continue
            table = pa.table(cols, schema=IADB_SCHEMA)
            save_raw_parquet(table, f"{node_id}-{cursor:05d}-{j:02d}")  # raw first
            rows_this_run += table.num_rows
            batches_this_run += 1

        seen.update(codes)
        cursor += 1
        save_state(node_id, {                                          # then state
            "schema_version": STATE_VERSION,
            "cat_fingerprint": fingerprint,
            "cat_count": len(categories),
            "cursor": cursor,
            "seen": sorted(seen),
            "cycle": cycle,
            "last_run_stats": {
                "rows": rows_this_run,
                "batches": batches_this_run,
                "series_seen": len(seen),
                "cursor": cursor,
            },
        })
        if cursor % 25 == 0:
            print(f"[iadb] {cursor}/{len(categories)} categories | "
                  f"{len(seen)} series | {rows_this_run} rows this run", flush=True)

    if cursor >= len(categories):
        print(f"[iadb] cycle {cycle} complete: {len(seen)} series, "
              f"{rows_this_run} rows / {batches_this_run} batches this run", flush=True)


# --------------------------------------------------------------------------
# Specs — one per entity-union entry (10 static files + 1 IADB corpus = 11).
# --------------------------------------------------------------------------
DOWNLOAD_SPECS: list[NodeSpec] = [
    NodeSpec(id=spec_id, fn=fetch_static_file, kind="download")
    for spec_id in STATIC_FILES
] + [
    NodeSpec(id=IADB_SPEC_ID, fn=fetch_iadb_observations, kind="download"),
]
