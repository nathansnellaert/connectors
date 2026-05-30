"""Climate Action Tracker — download step.

Three publishable products, three download specs:

  * country-emissions  — DRF REST, /data-portal/api/country-emissions/records/
  * sector-indicators  — DRF REST, /data-portal/api/records/
  * country-ratings    — NOT in the REST API; scraped from the embedded
                         <script id="data-props"> JSON on the country-ratings
                         explorer page (the value under "country_ratings" is a
                         JSON *string* that needs a second decode).

Fetch shape: stateless full re-pull. The whole corpus is tiny (~15k records +
~40 ratings, well under 1 MB of JSON) and the source publishes irregularly with
silent revisions to existing rows, so re-fetching everything every run and
overwriting is both cheap and correct — no watermark, no cursor, no state.

Raw format: NDJSON for all three. The REST records carry numeric `value`
fields that are sometimes null and free-text `comments`/`source` that come and
go; the ratings records are wide categorical blobs. NDJSON avoids brittle
parquet-schema drift and lets the transform step re-type on read.
"""
import json
import re

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_ndjson

# --- REST endpoints (DRF, page-style pagination, no auth) -------------------
EMISSIONS_URL = "https://climateactiontracker.org/data-portal/api/country-emissions/records/"
SECTOR_URL = "https://climateactiontracker.org/data-portal/api/records/"
# --- Country ratings explorer page (embedded JSON, scraped) -----------------
RATINGS_URL = "https://climateactiontracker.org/cat-data-explorer/country-ratings/"

PAGE_SIZE = 200          # DRF default is 20; ?page_size= honoured (research: tested 200)
MAX_PAGES = 2000         # safety ceiling; corpus is ~40 pages at size 200. Raises on hit.

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
def _get_json(url: str, params: dict | None = None):
    resp = get(url, params=params, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _get_text(url: str) -> str:
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.text


def _crawl_drf(url: str) -> list[dict]:
    """Walk a DRF page-paginated endpoint to exhaustion, return all records.

    Termination: follow the envelope's `next` URL until null. `count` on the
    first response is used as a sanity check on the total collected.
    """
    rows: list[dict] = []
    expected = None
    next_url = url
    params = {"page_size": PAGE_SIZE}
    pages = 0
    while next_url:
        pages += 1
        if pages > MAX_PAGES:
            raise RuntimeError(
                f"{url}: exceeded MAX_PAGES={MAX_PAGES} (collected {len(rows)} rows) "
                "- source grew past expectations, raise the cap deliberately"
            )
        # params only needed on the first request; `next` carries them onward.
        data = _get_json(next_url, params=params if next_url == url else None)
        if not isinstance(data, dict) or "results" not in data:
            raise RuntimeError(f"{url}: unexpected response shape (no DRF envelope)")
        if expected is None:
            expected = data.get("count")
        rows.extend(data["results"])
        next_url = data.get("next")
        if pages % 10 == 0:
            print(f"{url}: page {pages}, {len(rows)} rows so far", flush=True)
    if expected is not None and len(rows) != expected:
        print(
            f"WARNING {url}: collected {len(rows)} rows but envelope count={expected}",
            flush=True,
        )
    return rows


def fetch_country_emissions(node_id: str) -> None:
    asset = node_id
    rows = _crawl_drf(EMISSIONS_URL)
    save_raw_ndjson(rows, asset)
    print(f"{asset}: wrote {len(rows)} records", flush=True)


def fetch_sector_indicators(node_id: str) -> None:
    asset = node_id
    rows = _crawl_drf(SECTOR_URL)
    save_raw_ndjson(rows, asset)
    print(f"{asset}: wrote {len(rows)} records", flush=True)


def fetch_country_ratings(node_id: str) -> None:
    asset = node_id
    html = _get_text(RATINGS_URL)
    m = re.search(
        r'<script[^>]*id=["\']data-props["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not m:
        raise RuntimeError(
            f"{RATINGS_URL}: <script id='data-props'> not found - page structure changed"
        )
    props = json.loads(m.group(1).strip())
    ratings = props.get("country_ratings")
    if ratings is None:
        raise RuntimeError(
            f"{RATINGS_URL}: 'country_ratings' missing from data-props payload"
        )
    # The value is itself a JSON-encoded string (double-decode); tolerate the
    # case where the source one day inlines it as a real array.
    if isinstance(ratings, str):
        ratings = json.loads(ratings)
    if not isinstance(ratings, list):
        raise RuntimeError(
            f"{RATINGS_URL}: decoded 'country_ratings' is {type(ratings).__name__}, expected list"
        )
    save_raw_ndjson(ratings, asset)
    print(f"{asset}: wrote {len(ratings)} ratings", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id="climate-action-tracker-country-emissions",
        fn=fetch_country_emissions,
        kind="download",
    ),
    NodeSpec(
        id="climate-action-tracker-sector-indicators",
        fn=fetch_sector_indicators,
        kind="download",
    ),
    NodeSpec(
        id="climate-action-tracker-country-ratings",
        fn=fetch_country_ratings,
        kind="download",
    ),
]
