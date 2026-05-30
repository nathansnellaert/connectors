"""Apple top-charts download (rss.marketingtools.apple.com, API v2).

Mechanism: rss_marketing_tools — the public, unauth JSON REST API behind Apple's
RSS Generator UI. Each chart endpoint is:

    https://rss.marketingtools.apple.com/api/v2/{country}/{media}/{chart}/{limit}/{type}.json

The (media, type, chart) triples were discovered live from the generator's own
dropdown endpoints (`/apple/{media}/{country}/types` and
`/apple/{media}/{country}/{type}/feeds`) on 2026-05-30 — the research handoff's
feed-type names (top-grossing, top-songs, coming-soon, ...) are stale; the
authoritative current set is encoded in ENTITY_CONFIG below.

Snapshot-only source: each response is the *current* chart ranking, no history.
Apple publishes no historical rankings, so this fetch fn overwrites a fresh
per-entity snapshot every run (one row per country x chart x rank); downstream
Delta append turns the daily re-fetches into a time series. There is no
incremental filter to exploit and nothing to watermark — full re-pull is the
only correct shape (see "Choose your fetch shape", option 1).

Each entity-union entry is one chart family (one `type`), fetched across all
storefronts and all charts Apple exposes for that type. Item fields drift across
media (artistId/artistUrl/releaseDate/contentAdvisoryRating come and go, `genres`
is a nested list) so raw is written as NDJSON, not parquet.
"""

from datetime import datetime, timezone

import httpx
from ratelimit import limits, sleep_and_retry
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_ndjson

# --- chart catalog (entity -> media/type and the charts Apple exposes for it) ---
# Discovered live from the RSS generator's /types and /feeds endpoints; charts
# vary by media. music albums/songs/videos all live under the "most-played"
# chart, distinguished by `type` (the final URL segment). Re-verify against
# /apple/{media}/{country}/{type}/feeds if Apple changes the chart set.
ENTITY_CONFIG = {
    "apps-charts":            {"media": "apps",        "type": "apps",         "charts": ["top-free", "top-paid"]},
    "audio-books-charts":     {"media": "audio-books", "type": "audio-books",  "charts": ["top"]},
    "books-charts":           {"media": "books",       "type": "books",        "charts": ["top-free", "top-paid"]},
    "music-albums-charts":    {"media": "music",       "type": "albums",       "charts": ["most-played"]},
    "music-songs-charts":     {"media": "music",       "type": "songs",        "charts": ["most-played"]},
    "music-videos-charts":    {"media": "music",       "type": "music-videos", "charts": ["most-played"]},
    "podcast-episodes-charts":{"media": "podcasts",    "type": "podcast-episodes", "charts": ["top"]},
    "podcasts-charts":        {"media": "podcasts",    "type": "podcasts",     "charts": ["top", "top-subscriber"]},
}

# ISO 3166-1 alpha-2 storefront codes, pulled live from
# https://rss.marketingtools.apple.com/apple/{media}/storefronts on 2026-05-30
# (166 supported storefronts). Not every (country, media, chart) combination
# exists — missing ones 404 and are skipped per-combination.
COUNTRIES = [
    "dz", "ao", "ai", "ag", "ar", "am", "au", "at", "az", "bs", "bh", "bb", "by",
    "be", "bz", "bj", "bm", "bt", "bo", "ba", "bw", "br", "vg", "bg", "kh", "cm",
    "ca", "cv", "ky", "td", "cl", "cn", "co", "cr", "hr", "cy", "cz", "ci", "cd",
    "dk", "dm", "do", "ec", "eg", "sv", "ee", "sz", "fj", "fi", "fr", "ga", "gm",
    "ge", "de", "gh", "gr", "gd", "gt", "gw", "gy", "hn", "hk", "hu", "is", "in",
    "id", "iq", "ie", "il", "it", "jm", "jp", "jo", "kz", "ke", "kr", "xk", "kw",
    "kg", "la", "lv", "lb", "lr", "ly", "lt", "lu", "mo", "mg", "mw", "my", "mv",
    "ml", "mt", "mr", "mu", "mx", "fm", "md", "mn", "me", "ms", "ma", "mz", "mm",
    "na", "np", "nl", "nz", "ni", "ne", "ng", "mk", "no", "om", "pa", "pg", "py",
    "pe", "ph", "pl", "pt", "qa", "cg", "ro", "ru", "rw", "sa", "sn", "rs", "sc",
    "sl", "sg", "sk", "si", "sb", "za", "es", "lk", "kn", "lc", "vc", "sr", "se",
    "ch", "tw", "tj", "tz", "th", "to", "tt", "tn", "tm", "tc", "tr", "ae", "ug",
    "ua", "gb", "uy", "uz", "vu", "ve", "vn", "ye", "zm", "zw",
]

LIMIT = 200  # server accepts arbitrary values; 200 is the documented practical max
BASE = "https://rss.marketingtools.apple.com/api/v2"

# Item fields observed across the media families; kept flat per row. `genres`
# stays a nested list (NDJSON handles it). Missing fields become null on read.
_ITEM_FIELDS = (
    "id", "name", "kind", "artistName", "artistId", "artistUrl",
    "url", "artworkUrl100", "releaseDate", "contentAdvisoryRating",
)

_TRANSIENT_EXC = (
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
    httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError, httpx.ProxyError,
)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        # 503 is what Apple returns under burst load; retry it with the rest of 5xx.
        return code == 429 or 500 <= code < 600
    return False


class _NotFound(Exception):
    """Permanent: this (country, media, chart) combination doesn't exist."""


@sleep_and_retry
@limits(calls=3, period=1)  # ~3 req/s/process — well under what triggered 503s in probing
def _rate_limited_get(url: str) -> httpx.Response:
    return get(url, timeout=(10.0, 120.0))


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=2, max=120),
    reraise=True,
)
def _fetch_chart(country: str, media: str, chart: str, item_type: str) -> dict:
    """Return the parsed feed for one chart. Raises _NotFound (permanent) if the
    storefront/chart combination isn't published; transient errors are retried."""
    url = f"{BASE}/{country}/{media}/{chart}/{LIMIT}/{item_type}.json"
    resp = _rate_limited_get(url)  # rate limit re-applied on every retry attempt
    if resp.status_code == 404:
        # Genuine "not found" — this storefront/chart pair isn't published. Not
        # an error; the combination simply doesn't exist for this country.
        raise _NotFound(url)
    resp.raise_for_status()
    return resp.json()


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity = node_id[len("apple-"):]
    cfg = ENTITY_CONFIG[entity]
    media, item_type, charts = cfg["media"], cfg["type"], cfg["charts"]
    fetched_at = datetime.now(tz=timezone.utc).isoformat()

    rows: list[dict] = []
    missing = 0
    for i, country in enumerate(COUNTRIES, 1):
        for chart in charts:
            try:
                feed = _fetch_chart(country, media, chart, item_type)
            except _NotFound:
                missing += 1
                continue
            feed_meta = feed.get("feed", {})
            results = feed_meta.get("results", [])
            for rank, item in enumerate(results, 1):
                row = {
                    "entity": entity,
                    "country": country,
                    "media": media,
                    "type": item_type,
                    "chart": chart,
                    "rank": rank,
                    "feed_title": feed_meta.get("title"),
                    "feed_updated": feed_meta.get("updated"),
                    "fetched_at": fetched_at,
                    "genres": item.get("genres"),
                }
                for f in _ITEM_FIELDS:
                    row[f] = item.get(f)
                rows.append(row)
        if i % 25 == 0:
            print(f"[{asset}] {i}/{len(COUNTRIES)} storefronts, {len(rows)} rows so far",
                  flush=True)

    print(f"[{asset}] done: {len(rows)} rows across {len(COUNTRIES)} storefronts "
          f"x {len(charts)} chart(s); {missing} combos absent (404)", flush=True)
    if not rows:
        raise RuntimeError(f"{asset}: every storefront/chart returned no data — "
                           f"endpoint shape likely changed for media={media}")
    save_raw_ndjson(rows, asset)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"apple-{entity.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for entity in ENTITY_CONFIG
]
