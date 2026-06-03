"""Freedom House download node.

Mechanism: bulk_xlsx — each dataset is a single static Excel workbook served
from freedomhouse.org/sites/default/files/. One HTTP GET per dataset, full
corpus per file, no pagination, no auth.

Shape: stateless full re-pull (decision shape 1). Each workbook is the entire
dataset (~30KB-500KB); re-fetching every run is trivially cheap and picks up
the annual release for free. No watermark / cursor — flat-file downloads.

Raw format: the payload is an opaque .xlsx binary, so we persist the bytes
verbatim with save_raw_file(extension="xlsx"). Sheet parsing / typing is the
transform step's job (workbooks carry intro sheets + multiple data sheets with
non-trivial headers — see research notes).

URL-stability caveat (from research): the path segment embeds the publication
year-month (2025-02, 2024-05, 2020-02), so these URLs rotate with each annual
release. The set below is the current verified FIW-2025 / NIT-2024 / FOTP-2017
release; a future refresh that 404s must re-discover the dated paths by scraping
the data-download links on the report pages rather than guessing.
"""

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_file

# entity_id -> verified static workbook URL (point-in-time; see module docstring)
ENTITY_URLS = {
    "fiw-all-data": "https://freedomhouse.org/sites/default/files/2025-02/All_data_FIW_2013-2024.xlsx",
    "fiw-ratings-statuses": "https://freedomhouse.org/sites/default/files/2025-02/Country_and_Territory_Ratings_and_Statuses_FIW_1973-2024.xlsx",
    "fotp-public-data": "https://freedomhouse.org/sites/default/files/2020-02/FOTP1980-FOTP2017_Public-Data.xlsx",
    "nit-all-data": "https://freedomhouse.org/sites/default/files/2024-05/All_Data_Nations_in_Transit_NIT_2005-2024_For_website.xlsx",
}

# Minimum plausible workbook size — the smallest verified file (electoral
# democracies) is ~31KB and the ones we fetch are 61KB+. A response far below
# this is a truncated download or an error page mislabeled 200.
_MIN_BYTES = 10_000

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
def _fetch(url: str) -> bytes:
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.content


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity_id = node_id[len("freedom-house-"):]
    url = ENTITY_URLS[entity_id]  # KeyError here is a bug — let it raise

    content = _fetch(url)

    ct = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    # XLSX is a zip archive — every valid workbook starts with the PK signature.
    if len(content) < _MIN_BYTES or content[:2] != b"PK":
        raise AssertionError(
            f"{asset}: suspect payload from {url} "
            f"({len(content)} bytes, head={content[:4]!r}) — expected {ct}"
        )

    save_raw_file(content, asset, extension="xlsx")
    print(f"{asset}: saved {len(content)} bytes from {url}", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id="freedom-house-fiw-all-data", fn=fetch_one, kind="download"),
    NodeSpec(id="freedom-house-fiw-ratings-statuses", fn=fetch_one, kind="download"),
    NodeSpec(id="freedom-house-fotp-public-data", fn=fetch_one, kind="download"),
    NodeSpec(id="freedom-house-nit-all-data", fn=fetch_one, kind="download"),
]
