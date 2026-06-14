"""BLS LABSTAT bulk flat-file connector.

Fetch surface: the LABSTAT bulk channel at https://download.bls.gov/pub/time.series/.
Each entity is a per-survey directory (lowercase 2-char abbreviation) whose
``xx.data.*`` files hold the full observation history as tab-delimited fixed-column
ASCII (columns: series_id, year, period, value, footnote_codes).

Strategy (stateless full re-pull — shape 1): every refresh we enumerate the
survey directory's HTML autoindex, download ALL ``xx.data.*`` files, and stream
them into one parquet asset per survey. ``xx.data.0.Current`` is only a recent
subset (e.g. wp starts 1994) while the named/numbered partitions carry full
history (wp from 1967); downloading every data file and de-duplicating on
(series_id, year, period) in the transform guarantees complete coverage without
relying on a stored watermark. There is no incremental query for the bulk site,
so a full re-pull is the only correct mechanism (the maintain step gates whether
a survey re-runs via per-file Last-Modified).

Special case: the ``esbr`` survey ("Economy at a Glance" summary cards) has no
LABSTAT data files — only ``*.sf`` key/value indicator files — so it gets a
dedicated fetch/parse path.

Auth quirk: the BLS Akamai WAF returns 403 for default/empty/generic User-Agent
strings, so every request sends a contact-bearing User-Agent. No documented
numeric rate limit for the bulk site; the canonical retry/backoff handles the
Akamai throttle (429/5xx) since downloads are large sequential files, not a
high-rate small-request stream.
"""

import re

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
    SqlNodeSpec,
    get,
    get_client,
    configure_http,
    raw_parquet_writer,
    save_raw_ndjson,
)

# Contact-bearing UA — ASCII only (BLS Akamai 403s generic/empty UAs).
_UA = "subsets.io connector (nathan@subsets.io)"
_BASE = "https://download.bls.gov"
_CHUNK = 250_000  # rows buffered before each parquet row-group flush

# The entity union — every collect survey scored at/above the publish threshold.
ENTITY_IDS = [
    "ap", "bd", "ca", "ce", "ci", "cm", "cu", "cw", "cx", "ei",
    "esbr", "fa", "fm", "ip", "is", "jt", "kv", "la", "le", "ln",
    "lu", "mp", "nd", "oe", "or", "pc", "pr", "sm", "su", "tu",
    "wd", "wp", "ws",
]

# All-string raw schema — faithful capture; the transform is the typing/correctness
# gate (TRY_CAST + null filtering), so a stray non-numeric value never fails a fetch.
DATA_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("year", pa.string()),
    ("period", pa.string()),
    ("value", pa.string()),
    ("footnote_codes", pa.string()),
])

# Map of esbr .sf keys -> stable column names.
_ESBR_FIELDS = {
    "ID": "id",
    "INDICATOR": "indicator",
    "INDICATOR_SUBTITLE": "indicator_subtitle",
    "INDICATOR_LINK": "indicator_link",
    "SOURCE": "source",
    "SOURCE_LINK": "source_link",
    "QUOTE": "quote",
    "CURRENT": "current_value",
    "CURRENT_CAPTION": "current_caption",
    "CURRENT_UNITS": "current_units",
    "PREVIOUS": "previous_value",
    "PREVIOUS_CAPTION": "previous_caption",
    "PREVIOUS_UNITS": "previous_units",
    "AS_OF": "as_of",
}

_TRANSIENT_EXC = (
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
    httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError, httpx.ProxyError,
)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


_LINK_RE = re.compile(r'<A HREF="([^"]+)">([^<]+)</A>', re.IGNORECASE)


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_text(url: str) -> str:
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.text


def _list_files(xx: str, predicate) -> list[tuple[str, str]]:
    """Enumerate a survey directory's HTML autoindex.

    Returns sorted (name, absolute_url) pairs for entries matching ``predicate``.
    """
    text = _fetch_text(f"{_BASE}/pub/time.series/{xx}/")
    out = []
    for href, name in _LINK_RE.findall(text):
        if name == "[To Parent Directory]":
            continue
        if predicate(name):
            out.append((name, _BASE + href))
    return sorted(out)


def _make_batch(cols) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array(c, type=pa.string()) for c in cols],
        schema=DATA_SCHEMA,
    )


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _stream_data_file(url: str, writer) -> int:
    """Stream one LABSTAT data file line-by-line into the shared parquet writer.

    Memory is bounded to one ``_CHUNK`` of rows. On a transient mid-stream error
    the retry re-streams the whole file, which can write a few duplicate rows into
    the survey's parquet; the transform de-dups on (series_id, year, period), so
    this is safe.
    """
    sids: list[str] = []
    yrs: list[str] = []
    pers: list[str] = []
    vals: list[str] = []
    fns: list[str] = []
    total = 0
    client = get_client()
    with client.stream("GET", url, timeout=(10.0, 300.0)) as resp:
        resp.raise_for_status()
        first = True
        for line in resp.iter_lines():
            if first:  # header row
                first = False
                continue
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            sids.append(parts[0].strip())
            yrs.append(parts[1].strip())
            pers.append(parts[2].strip())
            vals.append(parts[3].strip())
            fns.append(parts[4].strip() if len(parts) > 4 else "")
            if len(sids) >= _CHUNK:
                writer.write_batch(_make_batch([sids, yrs, pers, vals, fns]))
                total += len(sids)
                sids.clear(); yrs.clear(); pers.clear(); vals.clear(); fns.clear()
        if sids:
            writer.write_batch(_make_batch([sids, yrs, pers, vals, fns]))
            total += len(sids)
    return total


def fetch_labstat(node_id: str) -> None:
    """Download every ``xx.data.*`` file for a LABSTAT survey into one parquet asset."""
    configure_http(headers={"User-Agent": _UA})
    xx = node_id[len("bls-"):]
    files = _list_files(xx, lambda n: ".data." in n)
    if not files:
        raise RuntimeError(f"{node_id}: no '.data.*' files found for survey {xx!r}")

    total = 0
    with raw_parquet_writer(node_id, DATA_SCHEMA) as writer:
        for i, (name, url) in enumerate(files, 1):
            rows = _stream_data_file(url, writer)
            total += rows
            print(f"[{node_id}] {i}/{len(files)} {name}: {rows} rows (cum {total})", flush=True)
    print(f"[{node_id}] DONE {total} rows from {len(files)} files", flush=True)


def _parse_sf(text: str) -> dict:
    """Parse a ``;KEY\\nvalue`` esbr indicator file into a normalized dict."""
    raw: dict[str, str] = {}
    key = None
    buf: list[str] = []
    for line in text.splitlines():
        if line.startswith(";"):
            if key is not None:
                raw[key] = "\n".join(buf).strip()
            key = line[1:].strip()
            buf = []
        else:
            buf.append(line)
    if key is not None:
        raw[key] = "\n".join(buf).strip()
    return {col: raw.get(src, "") for src, col in _ESBR_FIELDS.items()}


def fetch_esbr(node_id: str) -> None:
    """Download and parse the esbr ``*.sf`` indicator cards into one ndjson asset."""
    configure_http(headers={"User-Agent": _UA})
    files = _list_files("esbr", lambda n: n.endswith(".sf"))
    if not files:
        raise RuntimeError(f"{node_id}: no '.sf' indicator files found for esbr")
    rows = [_parse_sf(_fetch_text(url)) for _, url in files]
    save_raw_ndjson(rows, node_id)
    print(f"[{node_id}] DONE {len(rows)} esbr indicators", flush=True)


def _fn_for(eid: str):
    return fetch_esbr if eid == "esbr" else fetch_labstat


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"bls-{eid.lower().replace('_', '-')}",
        fn=_fn_for(eid),
        kind="download",
    )
    for eid in ENTITY_IDS
]


def _labstat_sql(dep_id: str) -> str:
    return f'''
        SELECT series_id, year, period, value, footnote_codes
        FROM (
            SELECT
                TRIM(series_id)                       AS series_id,
                TRY_CAST(TRIM(year) AS INTEGER)       AS year,
                TRIM(period)                          AS period,
                TRY_CAST(TRIM(value) AS DOUBLE)       AS value,
                NULLIF(TRIM(footnote_codes), '')      AS footnote_codes
            FROM "{dep_id}"
        )
        WHERE value IS NOT NULL
          AND year IS NOT NULL
          AND series_id <> ''
        QUALIFY row_number() OVER (
            PARTITION BY series_id, year, period ORDER BY value DESC
        ) = 1
    '''


_ESBR_SQL = '''
    SELECT
        id,
        indicator,
        NULLIF(indicator_subtitle, '')                          AS indicator_subtitle,
        NULLIF(indicator_link, '')                              AS indicator_link,
        NULLIF(source, '')                                      AS source,
        NULLIF(source_link, '')                                 AS source_link,
        NULLIF(quote, '')                                       AS quote,
        TRY_CAST(replace(current_value, ',', '') AS DOUBLE)     AS current_value,
        NULLIF(current_caption, '')                             AS current_caption,
        NULLIF(current_units, '')                               AS current_units,
        TRY_CAST(replace(previous_value, ',', '') AS DOUBLE)    AS previous_value,
        NULLIF(previous_caption, '')                            AS previous_caption,
        NULLIF(previous_units, '')                              AS previous_units,
        NULLIF(as_of, '')                                       AS as_of
    FROM "bls-esbr"
    WHERE id IS NOT NULL AND id <> ''
'''


def _sql_for(dep_id: str) -> str:
    return _ESBR_SQL if dep_id == "bls-esbr" else _labstat_sql(dep_id)


TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=_sql_for(s.id),
    )
    for s in DOWNLOAD_SPECS
]
