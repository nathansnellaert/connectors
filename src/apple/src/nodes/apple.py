"""Apple — top-chart connector.

Source: Apple RSS Marketing Tools JSON API (rss.marketingtools.apple.com/api/v2).
No auth, no documented rate limit. Each (country, media, feed_type) tuple is a
separate current-chart snapshot — Apple does NOT expose historical rankings, so
this is a snapshot-only source (fetch shape (e)): every run re-pulls the full
current chart for every entity and overwrites. History, if wanted, accrues from
repeated production refreshes downstream — not from stored watermarks here, so
there is no state/cursor logic.

URL template: {base}/{country}/{media}/{feed}/{limit}/{fname}.json

Per-entity full re-pull each run is cheap (≤ ~30 countries × ≤2 feeds × 200
items per entity, no pagination), so the stateless shape is the right one.
"""

from email.utils import parsedate_to_datetime

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, save_raw_ndjson

BASE = "https://rss.marketingtools.apple.com/api/v2"
LIMIT = 200  # server accepts arbitrary values; 200 captures full charts

# Storefronts to enumerate. ~30 major Apple storefronts — a manageable subset of
# the ~155 supported (per research). Country/feed combos that don't exist in a
# given storefront return 404 (audiobooks/some apps feeds aren't global) and are
# skipped per-combo; the asset is still populated from the storefronts that have
# the chart.
COUNTRIES = [
    "us", "gb", "ca", "au", "de", "fr", "it", "es", "nl", "se",
    "jp", "kr", "hk", "tw", "in", "br", "mx", "tr", "sa", "ae",
    "za", "sg", "id", "th", "vn", "ph", "my", "pl", "ie", "nz",
]

# Each collect entity maps to one media + filename + the feed_type(s) that exist
# for it on the live API (verified by probing — the catalog's guessed feed names
# like top-grossing / new-apps-we-love / coming-soon / top-podcast-episodes are
# NOT served and were dropped). entity_id is already lowercase-with-dashes.
ENTITY_CONFIG = {
    "apps-charts":             {"media": "apps",        "fname": "apps",            "feeds": ["top-free", "top-paid"]},
    "books-charts":            {"media": "books",       "fname": "books",           "feeds": ["top-free", "top-paid"]},
    "audio-books-charts":      {"media": "audio-books", "fname": "audio-books",     "feeds": ["top"]},
    "music-albums-charts":     {"media": "music",       "fname": "albums",          "feeds": ["most-played"]},
    "music-songs-charts":      {"media": "music",       "fname": "songs",           "feeds": ["most-played"]},
    "music-videos-charts":     {"media": "music",       "fname": "music-videos",    "feeds": ["most-played"]},
    "podcast-episodes-charts": {"media": "podcasts",    "fname": "podcast-episodes", "feeds": ["top"]},
    "podcasts-charts":         {"media": "podcasts",    "fname": "podcasts",        "feeds": ["top"]},
}

ENTITY_IDS = list(ENTITY_CONFIG.keys())

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
        # 503 is common from this CDN under load; 429/5xx are transient.
        return code == 429 or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_feed(url: str):
    """Fetch one chart endpoint. Returns parsed JSON dict, or None on a
    permanent 404 (feed/country combo not served)."""
    resp = get(url, timeout=(10.0, 120.0))
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def _snapshot_date(feed: dict) -> str | None:
    updated = feed.get("updated")
    if not updated:
        return None
    try:
        return parsedate_to_datetime(updated).date().isoformat()
    except (TypeError, ValueError):
        return None


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity_id = node_id[len("apple-"):]
    cfg = ENTITY_CONFIG[entity_id]
    media, fname, feeds = cfg["media"], cfg["fname"], cfg["feeds"]

    rows = []
    for country in COUNTRIES:
        for feed_type in feeds:
            url = f"{BASE}/{country}/{media}/{feed_type}/{LIMIT}/{fname}.json"
            data = _fetch_feed(url)
            if data is None:
                continue  # 404 — this storefront doesn't carry this feed
            feed = data.get("feed", {})
            results = feed.get("results") or []
            snap = _snapshot_date(feed)
            for rank, item in enumerate(results, start=1):
                genres = item.get("genres") or []
                rows.append({
                    "country": country,
                    "media": media,
                    "feed_type": feed_type,
                    "snapshot_date": snap,
                    "rank": rank,
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "kind": item.get("kind"),
                    "artist_name": item.get("artistName"),
                    "artist_id": item.get("artistId"),
                    "artist_url": item.get("artistUrl"),
                    "artwork_url": item.get("artworkUrl100"),
                    "release_date": item.get("releaseDate"),
                    "content_advisory_rating": item.get("contentAdvisoryRating"),
                    "url": item.get("url"),
                    "genre_ids": "|".join(g.get("genreId", "") for g in genres),
                    "genre_names": "|".join(g.get("name", "") for g in genres),
                })
        print(f"{asset}: {country} done, {len(rows)} rows so far", flush=True)

    save_raw_ndjson(rows, asset)
    print(f"{asset}: wrote {len(rows)} rows", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"apple-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]


TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f'''
            SELECT
                country,
                media,
                feed_type,
                CAST(snapshot_date AS DATE)          AS snapshot_date,
                CAST(rank AS INTEGER)                AS rank,
                CAST(id AS VARCHAR)                  AS id,
                name,
                kind,
                artist_name,
                CAST(artist_id AS VARCHAR)           AS artist_id,
                artist_url,
                artwork_url,
                TRY_CAST(release_date AS DATE)       AS release_date,
                content_advisory_rating,
                url,
                genre_ids,
                genre_names
            FROM "{s.id}"
            WHERE id IS NOT NULL
        ''',
    )
    for s in DOWNLOAD_SPECS
]
