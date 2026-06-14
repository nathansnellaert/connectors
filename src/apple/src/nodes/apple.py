"""Apple top-chart connector (rss.marketingtools.apple.com).

Mechanism: rss_marketing_tools — the public, unauth JSON REST API behind Apple's
RSS Generator. Each (country, media, feed_type) tuple is a separate chart
endpoint returning a CURRENT snapshot only (no history exposed by Apple). The
url template is

    https://rss.marketingtools.apple.com/api/v2/{country}/{media}/{feed_type}/{limit}/{tail}.json

Probing (2026-06-14) showed the live feed_type/tail names differ from the
research example endpoints, so the recipes below are the verified working set:

    apps              top-free, top-paid          tail=apps
    books             top-free, top-paid          tail=books
    audio-books       top                         tail=audio-books
    music             most-played                 tail=songs | albums | music-videos
    podcasts          top                         tail=podcasts | podcast-episodes

(top-grossing / new-apps-we-love / coming-soon / music top-songs etc. all 404 —
they are no longer served by this API.)

This is a SNAPSHOT-only source: each fetch re-pulls the current chart for every
storefront and overwrites the raw asset. Freshness/refresh cadence is the
maintain step's job; charts update ~daily. One DOWNLOAD_SPEC per collect entity;
each fetch fans out over a curated set of storefronts and the entity's feed
recipe(s), concatenating into one parquet asset.
"""
import json
from datetime import datetime, timezone

import httpx
import pyarrow as pa
from ratelimit import limits, sleep_and_retry
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, save_raw_parquet, save_state

# --- fetch surface ---------------------------------------------------------

BASE = "https://rss.marketingtools.apple.com/api/v2"
LIMIT = 200  # server accepts arbitrary values; full charts top out well under this.

# Curated set of major Apple storefronts (ISO 3166-1 alpha-2). Not every media is
# served in every storefront — missing (country, feed) combos return 404 and are
# skipped per-combo rather than failing the entity.
COUNTRIES = [
    "us", "gb", "ca", "au", "de", "fr", "it", "es", "nl", "se",
    "jp", "kr", "cn", "br", "mx", "in", "ru", "tr", "sa", "ae",
    "za", "id", "th", "vn", "ph", "sg", "hk", "tw", "nz", "ie",
]

# Per-entity fetch recipes: (media, feed_type, tail, feed_label). feed_label is
# the value stamped into the `feed_type` column so multi-feed entities stay
# distinguishable downstream.
ENTITY_RECIPES = {
    "apps-charts": [
        ("apps", "top-free", "apps", "top-free"),
        ("apps", "top-paid", "apps", "top-paid"),
    ],
    "books-charts": [
        ("books", "top-free", "books", "top-free"),
        ("books", "top-paid", "books", "top-paid"),
    ],
    "audio-books-charts": [
        ("audio-books", "top", "audio-books", "top"),
    ],
    "music-songs-charts": [
        ("music", "most-played", "songs", "most-played-songs"),
    ],
    "music-albums-charts": [
        ("music", "most-played", "albums", "most-played-albums"),
    ],
    "music-videos-charts": [
        ("music", "most-played", "music-videos", "most-played-music-videos"),
    ],
    "podcast-episodes-charts": [
        ("podcasts", "top", "podcast-episodes", "top-episodes"),
    ],
    "podcasts-charts": [
        ("podcasts", "top", "podcasts", "top-shows"),
    ],
}

ENTITY_IDS = list(ENTITY_RECIPES.keys())

SCHEMA = pa.schema([
    ("entity", pa.string()),
    ("country", pa.string()),
    ("feed_type", pa.string()),
    ("rank", pa.int32()),
    ("id", pa.string()),
    ("name", pa.string()),
    ("artist_name", pa.string()),
    ("artist_id", pa.string()),
    ("artist_url", pa.string()),
    ("kind", pa.string()),
    ("url", pa.string()),
    ("artwork_url", pa.string()),
    ("release_date", pa.string()),
    ("content_advisory_rating", pa.string()),
    ("primary_genre", pa.string()),
    ("genres_json", pa.string()),
    ("chart_title", pa.string()),
    ("feed_updated", pa.string()),
    ("observed_at", pa.timestamp("us", tz="UTC")),
])

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


@sleep_and_retry
@limits(calls=5, period=1)  # gentle per-process pacing; host throttles bursts with 503.
@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch_chart(url: str) -> dict:
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


def fetch_one(node_id: str) -> None:
    asset = node_id  # the spec id IS the asset name
    entity = node_id[len("apple-"):]
    recipes = ENTITY_RECIPES[entity]
    observed_at = datetime.now(tz=timezone.utc)

    rows = []
    skipped = 0
    for country in COUNTRIES:
        for media, feed_type, tail, feed_label in recipes:
            url = f"{BASE}/{country}/{media}/{feed_type}/{LIMIT}/{tail}.json"
            try:
                data = _fetch_chart(url)
            except httpx.HTTPStatusError as exc:
                # Permanent (4xx other than 429) or retry-exhausted transient:
                # one storefront/feed missing must not kill the whole entity.
                skipped += 1
                print(
                    f"[apple] skip {entity} {country}/{media}/{feed_type}: "
                    f"HTTP {exc.response.status_code} {url}",
                    flush=True,
                )
                continue

            feed = data.get("feed") or {}
            chart_title = feed.get("title")
            feed_updated = feed.get("updated")
            results = feed.get("results") or []
            for rank, item in enumerate(results, start=1):
                genres = item.get("genres") or []
                primary_genre = None
                if genres and isinstance(genres[0], dict):
                    primary_genre = genres[0].get("name")
                rows.append({
                    "entity": entity,
                    "country": country,
                    "feed_type": feed_label,
                    "rank": rank,
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "artist_name": item.get("artistName"),
                    "artist_id": item.get("artistId"),
                    "artist_url": item.get("artistUrl"),
                    "kind": item.get("kind"),
                    "url": item.get("url"),
                    "artwork_url": item.get("artworkUrl100"),
                    "release_date": item.get("releaseDate"),
                    "content_advisory_rating": item.get("contentAdvisoryRating"),
                    "primary_genre": primary_genre,
                    "genres_json": json.dumps(genres, ensure_ascii=False) if genres else None,
                    "chart_title": chart_title,
                    "feed_updated": feed_updated,
                    "observed_at": observed_at,
                })

    if not rows:
        raise RuntimeError(
            f"[apple] {entity}: every storefront/feed combo failed "
            f"({skipped} skipped) — no chart rows fetched."
        )

    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    save_raw_parquet(table, asset)
    save_state(asset, {
        "schema_version": 1,
        "last_success_at": observed_at.isoformat(),
        "last_run_stats": {
            "records": len(rows),
            "combos_skipped": skipped,
            "countries": len(COUNTRIES),
        },
    })
    print(f"[apple] {entity}: {len(rows)} rows, {skipped} combos skipped", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"apple-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]

# --- transforms: one published Delta table per entity ----------------------
# Uniform thin parse-and-type pass over each snapshot. overwrite() semantics mean
# the published table reflects the latest chart snapshot for every storefront.

TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"{s.id}-transform",
        deps=[s.id],
        sql=f"""
            SELECT
                entity,
                country,
                feed_type,
                CAST(rank AS INTEGER)                 AS rank,
                id,
                name,
                artist_name,
                artist_id,
                artist_url,
                kind,
                url,
                artwork_url,
                TRY_CAST(NULLIF(release_date, '') AS DATE) AS release_date,
                content_advisory_rating,
                primary_genre,
                genres_json,
                chart_title,
                feed_updated,
                CAST(observed_at AS TIMESTAMP)        AS observed_at
            FROM "{s.id}"
            WHERE id IS NOT NULL
        """,
    )
    for s in DOWNLOAD_SPECS
]
