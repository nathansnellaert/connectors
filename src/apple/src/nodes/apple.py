"""Apple top-chart RSS feeds (rss.marketingtools.apple.com).

Snapshot-only source: each (country, media, feed_type) endpoint returns the
*current* chart ranking — Apple exposes no history. The connector re-fetches
the full snapshot on every refresh; the transform/Delta layer accumulates the
daily snapshots into a time series. There is therefore no watermark to advance
(shape (e), snapshot-only): a refresh just overwrites the current raw asset.

One DOWNLOAD_SPEC per collect entity (8 chart families). Each fetch fn polls a
fixed set of storefronts (COUNTRIES) for that family's feed type(s) and writes
one NDJSON asset. NDJSON is deliberate: the result schema drifts across media
(podcasts lack releaseDate; books/music/audio-books add artistId/artistUrl;
music adds contentAdvisoryRating) and `genres` is a nested list of dicts.

The (media, feed_type, segment) taxonomy and limit ceiling below were verified
live against the API on 2026-05-28 — research's notes were partly stale (apps
top-grossing / new-apps-we-love, books coming-soon all 404; music uses
feed_type=most-played with the chart kind carried in the final URL segment, not
top-songs/top-albums; limit caps at 100, 200 returns HTTP 500).

Per-storefront isolation: a single (country, feed) failure must never sink the
whole spec. Some storefronts structurally lack a given media and Apple answers
with a *persistent* HTTP 500 — e.g. Books and Audiobooks in the China (cn)
storefront, which has no Apple Books / Audiobooks section. Other 500s are
transient (kr/vn/eg books charts were observed flipping 500->200 within
seconds) and are recovered by the retry decorator. So we retry every 5xx, and
if it still fails after the retries are exhausted we skip just that storefront
and keep going. A true global outage surfaces via the `fetched == 0` guard.
"""

import json
import httpx
from datetime import datetime, timezone
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from subsets_utils import NodeSpec, get, save_raw_ndjson, save_state

BASE_URL = "https://rss.marketingtools.apple.com/api/v2"
LIMIT = 100  # probed ceiling; the server returns HTTP 500 for limit > 100.

# entity_id -> (media, feed_types, result_segment). The result_segment is the
# final URL path component (.../{feed_type}/{limit}/{segment}.json) and is what
# distinguishes the three music charts, which all share feed_type=most-played.
ENTITY_SPECS = {
    "apps-charts":             ("apps",        ("top-free", "top-paid"), "apps"),
    "audio-books-charts":      ("audio-books", ("top",),                 "audio-books"),
    "books-charts":            ("books",       ("top-free", "top-paid"), "books"),
    "music-albums-charts":     ("music",       ("most-played",),         "albums"),
    "music-songs-charts":      ("music",       ("most-played",),         "songs"),
    "music-videos-charts":     ("music",       ("most-played",),         "music-videos"),
    "podcast-episodes-charts": ("podcasts",    ("top",),                 "podcast-episodes"),
    "podcasts-charts":         ("podcasts",    ("top",),                 "podcasts"),
}
ENTITY_IDS = list(ENTITY_SPECS.keys())

# Storefronts to poll each refresh. A curated set of ~40 major Apple storefronts
# (research suggested "top ~30 countries"); per-country gaps — a storefront that
# lacks a given chart family returns 404 — are skipped at fetch time, not fatal.
COUNTRIES = [
    "us", "gb", "ca", "au", "ie", "nz", "za",          # anglophone
    "de", "fr", "it", "es", "nl", "be", "at", "ch",    # western europe
    "se", "no", "dk", "fi", "pt", "pl", "gr",          # nordics + rest of EU
    "cz", "hu", "ro", "tr", "ru", "ua",
    "jp", "kr", "cn", "hk", "tw", "sg", "in", "id",    # asia-pacific
    "th", "vn", "ph", "my",
    "br", "mx", "ar", "cl", "co",                      # latin america
    "ae", "sa", "il", "eg",                            # middle east + africa
]

_TRANSIENT_EXC = (
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
    httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError, httpx.ProxyError,
)

# Failures that mean "this one storefront/feed is not serving this chart right
# now" rather than "the whole spec is broken". Caught per-(country, feed) in the
# fetch loop and skipped — only after the retry decorator below has already
# exhausted its attempts on the transient classes.
_SKIPPABLE_EXC = (httpx.HTTPStatusError, *_TRANSIENT_EXC, json.JSONDecodeError)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        # Apple returns sporadic 5xx under load and for some storefronts; retry
        # 429 + all 5xx. A *persistent* 5xx (storefront lacks this media) just
        # burns the retry budget and is then skipped per-storefront downstream.
        return code == 429 or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=2, max=30),
    reraise=True,
)
def _fetch_chart(country: str, media: str, feed_type: str, segment: str) -> dict:
    url = f"{BASE_URL}/{country}/{media}/{feed_type}/{LIMIT}/{segment}.json"
    resp = get(url, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


def fetch_one(entity_id: str) -> None:
    media, feed_types, segment = ENTITY_SPECS[entity_id]
    asset = f"apple-{entity_id.lower().replace('_', '-')}"
    # Freeze the fetch timestamp for the whole run so every row in this snapshot
    # carries the same fetched_at — the natural snapshot_date for the time series.
    fetched_at = datetime.now(tz=timezone.utc).isoformat()

    rows: list[dict] = []
    skipped: list[str] = []
    fetched = 0

    for feed_type in feed_types:
        for country in COUNTRIES:
            try:
                data = _fetch_chart(country, media, feed_type, segment)
            except _SKIPPABLE_EXC as e:
                # By the time we land here the retry decorator has exhausted its
                # attempts, so this (country, feed) is not serving this chart
                # right now: a 4xx feed/storefront gap, a 5xx that persisted
                # across every retry (e.g. cn Books/Audiobooks — China has no
                # such store), an unparseable body, or a network error that
                # never recovered. Skip just this storefront and keep going;
                # the `fetched == 0` guard below catches a true global outage.
                tag = e.response.status_code if isinstance(e, httpx.HTTPStatusError) else type(e).__name__
                skipped.append(f"{country}/{feed_type}:{tag}")
                continue

            feed = data.get("feed", {})
            results = feed.get("results", []) or []
            feed_updated = feed.get("updated")
            for rank, item in enumerate(results, start=1):
                rows.append({
                    "entity_id": entity_id,
                    "country": country,
                    "media": media,
                    "feed_type": feed_type,
                    "segment": segment,
                    "rank": rank,
                    "feed_updated": feed_updated,
                    "fetched_at": fetched_at,
                    **item,
                })

            fetched += 1
            if fetched % 10 == 0:
                print(f"[{entity_id}] {fetched} feeds fetched, {len(rows)} rows so far", flush=True)

    if not rows:
        # Every storefront/feed failed: the API shape changed or this chart
        # family was retired. Raise rather than write an empty asset.
        raise RuntimeError(
            f"{entity_id}: no rows from any of {len(COUNTRIES)} storefronts "
            f"x {feed_types} (skipped sample={skipped[:15]})"
        )

    save_raw_ndjson(rows, asset)  # raw first, always
    save_state(asset, {           # then observability state (snapshot: no watermark)
        "schema_version": 1,
        "last_run_stats": {
            "records": len(rows),
            "feeds_fetched": fetched,
            "skipped_count": len(skipped),
            "skipped": skipped,
            "fetched_at": fetched_at,
        },
    })
    print(
        f"[{entity_id}] done: {len(rows)} rows from {fetched} feeds "
        f"({len(skipped)} country/feed combos skipped)",
        flush=True,
    )


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"apple-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        args=(eid,),
        deps=(),
        kind="download",
    )
    for eid in ENTITY_IDS
]
