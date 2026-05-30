"""Cboe Global Markets — download step.

Six free, public, no-auth CSV corpora on the Cboe CDN (mechanisms from research:
bulk_indices + the five secondary fixed-corpus mechanisms). Each entity maps to one
DOWNLOAD_SPEC / one fetch fn.

Fetch shapes
------------
Five of the six entities are stateless full re-pulls: the corpora are small (a few
to ~2000 small CSVs) and cheap to re-fetch in full each refresh, which also picks up
same-day revisions for free.

  - index_daily_prices          : fetch GlobalIndices.csv catalog, then one
                                   {SYMBOL}_History.csv per symbol (~2000). Missing
                                   symbols return 403 (S3 AccessDenied) or tiny
                                   header-only files — both skipped. Streamed to one
                                   parquet (symbol,date,open,high,low,close); legacy
                                   2-column files (DATE,{SYMBOL}) map the lone value
                                   to close. Large -> streaming parquet writer.
  - equities_market_volume_*    : per-year market_history[_monthly]_{YYYY}.csv,
                                   2009..current year. -> ndjson.
  - cfe_vix_futures_archive     : frozen 2004-2013 archive, CFE_{CODE}{YY}_VX.csv
                                   over 12 month codes x 10 years. -> ndjson.
  - options_put_call_ratios     : 4 live series (+ optional _archive variants);
                                   2-line preamble before the DATE header; CALL/CALLS
                                   header drift handled by positional parse. -> ndjson.

One entity is incremental (shape 2a), because a full re-pull each run is genuinely
wasteful here:

  - vix_settlement_series       : per-settlement-date soq_vxs_{YYYYMMDD}.csv-dl. There
                                   is no machine-readable listing (the on-site page
                                   shows only a ~4-week rolling window) and a 404 ships
                                   a ~584KB HTML page. So discovery is "probe every
                                   weekday once". State remembers a monotonic
                                   `scanned_through` watermark plus the set of
                                   discovered `found_dates`; each refresh only probes
                                   the small forward window (+14d overlap for late
                                   publishes) and re-fetches the known dates (cheap,
                                   ~20MB) to rebuild the full single ndjson asset.
                                   Backfill (empty state) probes every weekday back to
                                   VIX_SOQ_MIN once. -> ndjson.
"""

import csv
from datetime import datetime, timedelta, timezone

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
    save_state,
    save_raw_ndjson,
    raw_parquet_writer,
)

STATE_VERSION = 1

# --- source constants ---------------------------------------------------------
CATALOG_URL = "https://cdn.cboe.com/api/global/us_indices/definitions/GlobalIndices.csv"
INDEX_HISTORY_BASE = "https://cdn.cboe.com/api/global/us_indices/daily_prices"
EQUITIES_BASE = "https://cdn.cboe.com/resources/us/equities/market-statistics/historical-market-volume"
PUTCALL_BASE = "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios"
CFE_ARCHIVE_BASE = "https://cdn.cboe.com/resources/futures/archive/volume-and-price"
SOQ_BASE = "https://www.cboe.com/us/futures/market_statistics/vix_settlement_series"

# CFE VIX futures archive is a frozen 2004-2013 corpus (decided 2026-05; research:
# "frozen historical archive ... 2004-2013 only"). Standard futures month-code letters.
CFE_YEARS = range(2004, 2014)
CFE_MONTH_CODES = "FGHJKMNQUVXZ"

EQUITIES_START_YEAR = 2009  # earliest published year per research/probe

# Put/call: (logical series, base filename). Each tried as live and _archive variant.
PUTCALL_SERIES = (
    ("total", "totalpc"),
    ("equity", "equitypc"),
    ("index", "indexpc"),
    ("vix", "vixpc"),
)

# VIX SOQ files first appear ~2019-10 (probed: 2019-09-18 -> 404, 2019-10-16 -> 200).
# Start one month earlier as a safe floor for the one-time backfill scan.
VIX_SOQ_MIN = "20190901"
VIX_SOQ_OVERLAP_DAYS = 14  # re-scan recent window for late-published files

INDEX_SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("date", pa.string()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
])


# --- transport ----------------------------------------------------------------
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
    wait=wait_exponential(min=2, max=60),
    reraise=True,
)
def _fetch_text(url: str) -> str:
    """GET returning decoded text. Transient failures retried; 4xx raise immediately."""
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.text


def _fetch_optional(url: str):
    """Like _fetch_text but returns None on a permanent 'absent' status (403/404).

    The Cboe CDN returns 403 (S3 AccessDenied) for absent index/futures symbols and
    404 for absent settlement dates — both are normal skips, not errors.
    """
    try:
        return _fetch_text(url)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (403, 404):
            return None
        raise


def _f(value) -> float | None:
    """Parse a CSV cell to float, or None if blank/unparseable."""
    s = (value or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


# --- index_daily_prices -------------------------------------------------------
def _index_url(symbol: str) -> str:
    return f"{INDEX_HISTORY_BASE}/{symbol}_History.csv"


def _parse_catalog_symbols(text: str) -> list[str]:
    reader = csv.DictReader(text.splitlines())
    symbols = []
    for row in reader:
        sym = (row.get("Symbol") or "").strip()
        if sym:
            symbols.append(sym)
    return symbols


def _parse_index(text: str, symbol: str):
    """Return column-dict for INDEX_SCHEMA, or None if the file is empty/odd-shaped.

    Standard files: DATE,OPEN,HIGH,LOW,CLOSE. Legacy single-value files: DATE,{SYMBOL}
    where the lone value is treated as close.
    """
    rows = list(csv.reader(text.splitlines()))
    if len(rows) < 2:
        return None
    header = [h.strip().upper() for h in rows[0]]
    data = rows[1:]
    dates, opens, highs, lows, closes = [], [], [], [], []

    if {"OPEN", "HIGH", "LOW", "CLOSE", "DATE"}.issubset(header):
        di = header.index("DATE")
        oi, hi, li, ci = (header.index(k) for k in ("OPEN", "HIGH", "LOW", "CLOSE"))
        for r in data:
            if len(r) <= ci or not r[di].strip():
                continue
            dates.append(r[di].strip())
            opens.append(_f(r[oi]))
            highs.append(_f(r[hi]))
            lows.append(_f(r[li]))
            closes.append(_f(r[ci]))
    elif len(header) == 2:
        for r in data:
            if len(r) < 2 or not r[0].strip():
                continue
            dates.append(r[0].strip())
            opens.append(None)
            highs.append(None)
            lows.append(None)
            closes.append(_f(r[1]))
    else:
        print(f"  index: unexpected header for {symbol}: {header}", flush=True)
        return None

    if not dates:
        return None
    n = len(dates)
    return {
        "symbol": [symbol] * n,
        "date": dates,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
    }


def fetch_index(node_id: str) -> None:
    asset = node_id
    catalog = _fetch_text(CATALOG_URL)  # required — a failure here is a real error
    symbols = _parse_catalog_symbols(catalog)
    if not symbols:
        raise AssertionError("GlobalIndices.csv yielded no symbols")
    if len(symbols) > 50_000:  # safety ceiling: surface unexpected catalog growth
        raise AssertionError(f"catalog unexpectedly large: {len(symbols)} symbols")

    total_rows = with_data = skipped = 0
    with raw_parquet_writer(asset, INDEX_SCHEMA) as writer:
        for i, sym in enumerate(symbols, 1):
            text = _fetch_optional(_index_url(sym))
            if text is None:
                skipped += 1
                continue
            cols = _parse_index(text, sym)
            if cols is None:
                continue
            writer.write_table(pa.table(cols, schema=INDEX_SCHEMA))
            with_data += 1
            total_rows += len(cols["date"])
            if i % 250 == 0:
                print(
                    f"  index: {i}/{len(symbols)} symbols, "
                    f"{with_data} with data, {total_rows} rows",
                    flush=True,
                )
    print(
        f"  index: done — {len(symbols)} symbols, {with_data} with data, "
        f"{skipped} absent, {total_rows} rows",
        flush=True,
    )


# --- equities_market_volume (daily / monthly) ---------------------------------
def _fetch_equities(node_id: str, monthly: bool) -> None:
    asset = node_id
    current_year = datetime.now(timezone.utc).year
    infix = "market_history_monthly_" if monthly else "market_history_"
    rows = []
    fetched_years = 0
    for year in range(EQUITIES_START_YEAR, current_year + 1):
        url = f"{EQUITIES_BASE}/{infix}{year}.csv"
        text = _fetch_optional(url)
        if text is None:
            continue
        n = 0
        for row in csv.DictReader(text.splitlines()):
            if not any((v or "").strip() for v in row.values()):
                continue
            row["_year"] = str(year)
            rows.append(row)
            n += 1
        if n:
            fetched_years += 1
    print(
        f"  {asset}: {fetched_years} years, {len(rows)} rows",
        flush=True,
    )
    save_raw_ndjson(rows, asset)


def fetch_equities_daily(node_id: str) -> None:
    _fetch_equities(node_id, monthly=False)


def fetch_equities_monthly(node_id: str) -> None:
    _fetch_equities(node_id, monthly=True)


# --- cfe_vix_futures_archive --------------------------------------------------
def fetch_cfe_archive(node_id: str) -> None:
    asset = node_id
    rows = []
    files = 0
    for year in CFE_YEARS:
        yy = f"{year % 100:02d}"
        for code in CFE_MONTH_CODES:
            contract = f"{code}{yy}"
            url = f"{CFE_ARCHIVE_BASE}/CFE_{contract}_VX.csv"
            text = _fetch_optional(url)
            if text is None:
                continue
            n = 0
            for row in csv.DictReader(text.splitlines()):
                if not (row.get("Trade Date") or "").strip():
                    continue
                row["_contract"] = contract
                rows.append(row)
                n += 1
            if n:
                files += 1
    print(f"  {asset}: {files} contract files, {len(rows)} rows", flush=True)
    save_raw_ndjson(rows, asset)


# --- options_put_call_ratios --------------------------------------------------
def _parse_putcall(text: str, series: str, variant: str) -> list[dict]:
    """Skip the 2-line disclaimer/product preamble, read positional columns.

    Header is DATE,CALL[S],PUT[S],TOTAL,P/C Ratio (singular vs plural varies by file),
    so parse by position rather than by header name.
    """
    reader = csv.reader(text.splitlines())
    out = []
    started = False
    for r in reader:
        if not started:
            if r and r[0].strip().upper() == "DATE":
                started = True
            continue
        if len(r) < 5 or not r[0].strip():
            continue
        out.append({
            "series": series,
            "variant": variant,
            "date": r[0].strip(),
            "calls": r[1].strip(),
            "puts": r[2].strip(),
            "total": r[3].strip(),
            "pc_ratio": r[4].strip(),
        })
    return out


def fetch_put_call(node_id: str) -> None:
    asset = node_id
    rows = []
    files = 0
    for series, base in PUTCALL_SERIES:
        for variant, suffix in (("live", ""), ("archive", "_archive")):
            url = f"{PUTCALL_BASE}/{base}{suffix}.csv"
            text = _fetch_optional(url)
            if text is None:
                continue
            parsed = _parse_putcall(text, series, variant)
            if parsed:
                rows.extend(parsed)
                files += 1
    print(f"  {asset}: {files} files, {len(rows)} rows", flush=True)
    save_raw_ndjson(rows, asset)


# --- vix_settlement_series (incremental) --------------------------------------
def _soq_url(yyyymmdd: str) -> str:
    return f"{SOQ_BASE}/{yyyymmdd[:4]}/{yyyymmdd[4:6]}/soq_vxs_{yyyymmdd}.csv-dl"


def _parse_soq(text: str, yyyymmdd: str) -> list[dict]:
    out = []
    reader = csv.DictReader(text.splitlines())
    if not reader.fieldnames or "Date" not in reader.fieldnames:
        return out
    for row in reader:
        if not any((v or "").strip() for v in row.values()):
            continue
        row["_settlement_date"] = yyyymmdd
        out.append(row)
    return out


def fetch_vix_settlement(node_id: str) -> None:
    asset = node_id
    state = load_state(asset)
    if state.get("schema_version") != STATE_VERSION:
        state = {}

    found = set(state.get("found_dates", []))
    today = datetime.now(timezone.utc).date()
    floor = datetime.strptime(VIX_SOQ_MIN, "%Y%m%d").date()
    scanned_through = state.get("scanned_through")
    if scanned_through:
        start = datetime.strptime(scanned_through, "%Y%m%d").date() - timedelta(
            days=VIX_SOQ_OVERLAP_DAYS
        )
        start = max(start, floor)
    else:
        start = floor

    fetched: dict[str, list[dict]] = {}

    # 1. Discover new settlement dates by probing each weekday in the window once.
    new_dates = 0
    probed = 0
    day = start
    while day <= today:
        if day.weekday() < 5:  # Mon-Fri
            ds = day.strftime("%Y%m%d")
            if ds not in found:
                probed += 1
                text = _fetch_optional(_soq_url(ds))
                if text is not None:
                    found.add(ds)
                    fetched[ds] = _parse_soq(text, ds)
                    new_dates += 1
                if probed % 200 == 0:
                    print(
                        f"  vix-soq: probed {probed} candidate days, "
                        f"{len(found)} found so far",
                        flush=True,
                    )
        day += timedelta(days=1)

    # 2. Re-fetch previously-known dates (cheap, ~20MB) to rebuild the full asset
    #    and pick up any revisions.
    for ds in sorted(found):
        if ds in fetched:
            continue
        text = _fetch_optional(_soq_url(ds))
        fetched[ds] = _parse_soq(text, ds) if text is not None else []

    rows = [row for ds in sorted(fetched) for row in fetched[ds]]

    # Write raw BEFORE advancing state.
    save_raw_ndjson(rows, asset)
    save_state(asset, {
        "schema_version": STATE_VERSION,
        "scanned_through": today.strftime("%Y%m%d"),
        "found_dates": sorted(found),
        "last_run_stats": {
            "records": len(rows),
            "found_dates": len(found),
            "new_dates": new_dates,
        },
    })
    print(
        f"  {asset}: {len(found)} settlement dates ({new_dates} new), {len(rows)} rows",
        flush=True,
    )


# --- specs --------------------------------------------------------------------
DOWNLOAD_SPECS = [
    NodeSpec(id="cboe-global-markets-cfe-vix-futures-archive", fn=fetch_cfe_archive, kind="download"),
    NodeSpec(id="cboe-global-markets-equities-market-volume-daily", fn=fetch_equities_daily, kind="download"),
    NodeSpec(id="cboe-global-markets-equities-market-volume-monthly", fn=fetch_equities_monthly, kind="download"),
    NodeSpec(id="cboe-global-markets-index-daily-prices", fn=fetch_index, kind="download"),
    NodeSpec(id="cboe-global-markets-options-put-call-ratios", fn=fetch_put_call, kind="download"),
    NodeSpec(id="cboe-global-markets-vix-settlement-series", fn=fetch_vix_settlement, kind="download"),
]
