"""Apple — top-chart snapshots from the RSS Marketing Tools API.

Mechanism: rss_marketing_tools (https://rss.marketingtools.apple.com/api/v2/).
Each (country, media, feed_type) tuple is a separate top-chart endpoint:

    {BASE}/{country}/{media}/{feed_type}/{limit}/{result_kind}.json

This is a snapshot-only source — Apple exposes only the *current* ranking,
never history. The connector re-fetches the full chart set on every refresh
(charts update ~daily) and downstream Delta append builds the time series.
State holds no watermark because the API exposes none; idempotency is the
per-entity `raw_asset_exists(..., max_age_days=1)` short-circuit, which also
lets a crashed run resume — entities saved before the crash are skipped.

One DOWNLOAD_SPEC per collect entity; each entity fans out over a fixed set
of storefront countries and the feed types valid for its media. Country/feed
combos that aren't valid storefront charts return HTTP 404 and are skipped
per-combo (a permanent error for that combo, not a spec-level failure).

Endpoint paths were probed live before authoring — research's claimed
feed_type names for music (`top-songs`/`top-albums`) and books (`coming-soon`)
404; the verified working values are below (music uses `most-played`,
books uses `top-free`/`top-paid`, podcasts/audio-books use `top`).
"""

from datetime import datetime, timezone

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from subsets_utils import NodeSpec, get, save_raw_json, raw_asset_exists, load_state, save_state

BASE = "https://rss.marketingtools.apple.com/api/v2"
LIMIT = 100  # chart depth; UI presets stop at 50, server accepts deeper. Keeps payloads modest.

# Per-entity fetch config. `result_kind` is the final URL path segment;
# `feed_types` are the feed slugs verified to return HTTP 200 for that media.
ENTITY_CONFIG = {
    "apps-charts":             {"media": "apps",        "result_kind": "apps",             "feed_types": ["top-free", "top-paid"]},
    "audio-books-charts":      {"media": "audio-books", "result_kind": "audio-books",      "feed_types": ["top"]},
    "books-charts":            {"media": "books",       "result_kind": "books",            "feed_types": ["top-free", "top-paid"]},
    "music-albums-charts":     {"media": "music",       "result_kind": "albums",           "feed_types": ["most-played"]},
    "music-songs-charts":      {"media": "music",       "result_kind": "songs",            "feed_types": ["most-played"]},
    "music-videos-charts":     {"media": "music",       "result_kind": "music-videos",     "feed_types": ["most-played"]},
    "podcast-episodes-charts": {"media": "podcasts",    "result_kind": "podcast-episodes", "feed_types": ["top"]},
    "podcasts-charts":         {"media": "podcasts",    "result_kind": "podcasts",         "feed_types": ["top"]},
}

# Entity union — copied from the entity_union.json coverage target.
ENTITY_IDS = [
    "apps-charts",
    "audio-books-charts",
    "books-charts",
    "music-albums-charts",
    "music-songs-charts",
    "music-videos-charts",
    "podcast-episodes-charts",
    "podcasts-charts",
]

# Storefront countries to fetch — ~22 major Apple storefronts (ISO 3166-1
# alpha-2) spanning every region. A focused set, not all ~155 storefronts:
# keeps the per-refresh crawl to a few hundred requests so the run stays
# well inside the materialize window. Missing media/feed combos 404 and are
# skipped per-combo.
COUNTRIES = [
    "us", "gb", "ca", "au",
    "de", "fr", "it", "es", "nl", "se",
    "br", "mx",
    "jp", "kr", "cn", "hk", "tw", "sg", "in", "id",
    "ru", "za",
]

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


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
def _fetch(url: str) -> dict:
    resp = get(url, timeout=(10.0, 60.0))  # (connect, read)
    resp.raise_for_status()
    return resp.json()


def fetch_one(entity_id: str) -> None:
    """Fetch the current top-chart snapshot for one entity across all
    configured storefronts, accumulate annotated rows, save as raw JSON."""
    cfg = ENTITY_CONFIG[entity_id]
    asset = f"apple-{entity_id.lower().replace('_', '-')}"

    # Snapshot-only source: Apple refreshes charts ~daily per
    # rss.marketingtools.apple.com, so re-fetch at most once per day.
    if raw_asset_exists(asset, ext="json", max_age_days=1):  # Apple charts update ~daily per rss.marketingtools.apple.com
        return

    snapshot = datetime.now(tz=timezone.utc).isoformat()
    combos = [(c, ft) for c in COUNTRIES for ft in cfg["feed_types"]]
    records: list[dict] = []
    failures: list[tuple[str, str]] = []

    for i, (country, feed_type) in enumerate(combos, 1):
        url = f"{BASE}/{country}/{cfg['media']}/{feed_type}/{LIMIT}/{cfg['result_kind']}.json"
        try:
            payload = _fetch(url)
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            # 4xx (except 429, which the retry handles) is permanent for this
            # combo — usually a storefront that doesn't carry this chart.
            if 400 <= code < 500 and code != 429:
                print(f"[apple] skip {url} -> HTTP {code}", flush=True)
                continue
            failures.append((url, f"HTTP {code}"))
            print(f"[apple] FAIL {url} -> HTTP {code}", flush=True)
            continue
        except httpx.HTTPError as exc:
            # Transient failure that exhausted the retry budget.
            failures.append((url, type(exc).__name__))
            print(f"[apple] FAIL {url} -> {type(exc).__name__}", flush=True)
            continue

        feed = payload.get("feed", {}) or {}
        results = feed.get("results", []) or []
        for rank, item in enumerate(results, 1):
            records.append({
                "country": country,
                "media": cfg["media"],
                "feed_type": feed_type,
                "feed_title": feed.get("title"),
                "feed_updated": feed.get("updated"),
                "snapshot_date": snapshot,
                "rank": rank,
                "item": item,
            })
        if i % 10 == 0:
            print(f"[apple] {entity_id}: {i}/{len(combos)} combos, {len(records)} rows", flush=True)

    # A handful of 404 skips is normal; widespread transient failure is not.
    if len(failures) > len(combos) // 2:
        raise RuntimeError(
            f"{entity_id}: {len(failures)}/{len(combos)} combos failed transiently "
            f"- API likely down, aborting rather than persisting a partial chart set"
        )
    if not records:
        raise RuntimeError(
            f"{entity_id}: no chart rows fetched from any of {len(combos)} (country, feed) combos"
        )

    # Raw before state — a crash between them loses at worst one snapshot,
    # never creates a phantom completion.
    save_raw_json(records, asset)

    state = load_state(asset)
    if state.get("schema_version") != 1:
        state = {}
    state["schema_version"] = 1
    state["last_success_at"] = snapshot
    state["last_run_stats"] = {
        "records": len(records),
        "combos_attempted": len(combos),
        "combos_failed": len(failures),
        "snapshot_date": snapshot,
    }
    save_state(asset, state)
    print(f"[apple] {entity_id}: saved {len(records)} rows ({len(failures)} failed combos)", flush=True)


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
