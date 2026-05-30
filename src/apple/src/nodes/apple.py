"""Apple top-charts download (rss.marketingtools.apple.com, API v2).

Mechanism: rss_marketing_tools — the public, unauth JSON REST API behind Apple's
RSS Generator UI. Each chart endpoint is:

    https://rss.marketingtools.apple.com/api/v2/{country}/{media}/{chart}/{limit}/{type}.json

The (media, chart, type) triples in ENTITY_CONFIG were verified live on 2026-05-30
to return 200 with catalog metadata for the US storefront. (The research handoff's
feed-type names were partly stale: `top-grossing`, `top-albums`, `top-songs`,
`coming-soon` all 404; the authoritative current set is encoded below — music
charts live under `most-played` distinguished by the final `type` segment, and
audio-books / podcasts use a bare `top`.)

Snapshot-only source: each response is the *current* chart ranking, no history.
Apple publishes no historical rankings, so this fetch fn overwrites a fresh
per-entity snapshot every run (one row per country x chart x rank); downstream
Delta append turns the daily re-fetches into a time series. There is no
incremental filter to exploit and nothing to watermark — full re-pull is the
only correct shape (see "Choose your fetch shape", option 1).

Per-storefront availability differs by media. Not every (country, media) pair is
published: e.g. the China (`cn`) storefront returns a deterministic HTTP 500 for
`books` / `audio-books` (those media aren't sold there), and long-tail
storefronts 404 some charts. These are NOT errors — a single storefront/chart
that is permanently unavailable (404, or a 500 that survives retries) is logged
and skipped, and the entity continues with the storefronts that do work. This is
the key fix over the prior attempt: the prior code let a `cn` 500 propagate out
of the fetch fn, which the orchestrator treats as a node failure and aborts the
whole DAG. An entity only fails loudly if EVERY storefront/chart yields nothing
(genuine endpoint-shape change).

Scope: each entity is one chart family, fetched across a curated set of ~43 major
storefronts (research suggested "top ~30 countries"). The full ~155-storefront
corpus was deliberately NOT enumerated — marginal coverage of long-tail
storefronts is low and request volume scales with it. Item fields drift across
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

# --- chart catalog (entity -> media + the charts/type-segments Apple exposes) ---
# Every (media, chart, type) triple below was verified live (200 + 100 items) on
# the US storefront, 2026-05-30. Music albums/songs/music-videos all live under
# the "most-played" chart, distinguished by the final `type` URL segment.
ENTITY_CONFIG = {
    "apps-charts":             {"media": "apps",        "type": "apps",             "charts": ["top-free", "top-paid"]},
    "audio-books-charts":      {"media": "audio-books", "type": "audio-books",      "charts": ["top"]},
    "books-charts":            {"media": "books",       "type": "books",            "charts": ["top-free", "top-paid"]},
    "music-albums-charts":     {"media": "music",       "type": "albums",           "charts": ["most-played"]},
    "music-songs-charts":      {"media": "music",       "type": "songs",            "charts": ["most-played"]},
    "music-videos-charts":     {"media": "music",       "type": "music-videos",     "charts": ["most-played"]},
    "podcast-episodes-charts": {"media": "podcasts",    "type": "podcast-episodes", "charts": ["top"]},
    "podcasts-charts":         {"media": "podcasts",    "type": "podcasts",         "charts": ["top"]},
}

# ~43 major App Store / iTunes storefronts (ISO 3166-1 alpha-2), covering the
# bulk of catalog/ranking activity. Not every (country, media, chart) exists —
# missing ones 404, and some (e.g. cn books/audio-books) deterministically 500;
# both are skipped per-combination, not fatal.
COUNTRIES = [
    "us", "gb", "ca", "au", "ie", "nz",                          # anglophone
    "de", "fr", "it", "es", "nl", "se", "no", "dk", "fi",        # W/N europe
    "ch", "at", "be", "pt", "pl", "ru", "tr",                    # rest of europe
    "jp", "kr", "cn", "hk", "tw", "sg", "in", "id", "th", "my",  # asia-pacific
    "ph", "vn",
    "br", "mx", "ar", "cl", "co",                                # latam
    "za", "ae", "sa", "il",                                      # mea
]

LIMIT = 100  # verified live max: limit=200 returns a deterministic HTTP 500 across
             # every media; 100 is the largest value the server actually serves.
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
        # 429 throttle + 5xx. Apple returns 503 under burst and a *deterministic*
        # 500 for unavailable (country, media) pairs — we retry both here, and the
        # caller treats a 5xx that survives all retries as a permanent skip for
        # that one storefront/chart rather than failing the whole entity.
        return code == 429 or 500 <= code < 600
    return False


class _SkipCombo(Exception):
    """Permanent: this (country, media, chart) combination isn't published (404)."""


@sleep_and_retry
@limits(calls=2, period=1)  # ~2 req/s/process; gentle enough to avoid 503 churn
def _rate_limited_get(url: str) -> httpx.Response:
    return get(url, timeout=(10.0, 120.0))


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(4),
    wait=wait_exponential(min=2, max=30),
    reraise=True,
)
def _fetch_chart(country: str, media: str, chart: str, item_type: str) -> dict:
    """Return the parsed feed for one chart. Raises _SkipCombo (permanent) on a
    404; transient/5xx errors are retried, and a 5xx that survives all retries
    re-raises HTTPStatusError for the caller to treat as a per-combo skip."""
    url = f"{BASE}/{country}/{media}/{chart}/{LIMIT}/{item_type}.json"
    resp = _rate_limited_get(url)  # rate limit re-applied on every retry attempt
    if resp.status_code == 404:
        raise _SkipCombo(url)
    resp.raise_for_status()
    return resp.json()


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity = node_id[len("apple-"):]
    cfg = ENTITY_CONFIG[entity]
    media, item_type, charts = cfg["media"], cfg["type"], cfg["charts"]
    fetched_at = datetime.now(tz=timezone.utc).isoformat()

    rows: list[dict] = []
    attempted = 0
    skipped = 0
    for i, country in enumerate(COUNTRIES, 1):
        for chart in charts:
            attempted += 1
            try:
                feed = _fetch_chart(country, media, chart, item_type)
            except _SkipCombo as e:
                # 404 — this storefront/chart pair isn't published for this country.
                skipped += 1
                continue
            except httpx.HTTPStatusError as e:
                # A 5xx that survived all retries — typically a deterministic
                # "media not available in this storefront" (e.g. cn books/500).
                # Skip THIS combo; do not fail the whole entity.
                skipped += 1
                print(f"[{asset}] skip {country}/{media}/{chart}: "
                      f"{e.response.status_code} after retries", flush=True)
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
        if i % 10 == 0:
            print(f"[{asset}] {i}/{len(COUNTRIES)} storefronts, {len(rows)} rows so far",
                  flush=True)

    print(f"[{asset}] done: {len(rows)} rows; {attempted} combos attempted, "
          f"{skipped} skipped (404/5xx-unavailable)", flush=True)

    if not rows:
        # Genuine total failure: every storefront/chart yielded nothing. This is
        # an endpoint-shape change for this media, not normal per-store gaps.
        raise RuntimeError(
            f"{asset}: 0 rows from {attempted} storefront/chart combos — "
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
