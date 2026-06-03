"""FIFA download nodes — api.fifa.com/api/v3 (undocumented public JSON API).

Two collect entities, one DOWNLOAD_SPEC each:

  * matches    — every match record across every competition/season/stage
  * standings   — league/group standings per (competition, season, stage)

Access strategy (from research handoff + live probing)
------------------------------------------------------
The v3 API is a CosmosDB-backed gateway. The *global* /calendar/matches stream
is sorted ascending by Date but cannot be paged to completion: the ascending
`from=<ISO date>` cursor walks one CosmosDB partition range and then returns a
literal JSON `null` body at a partition boundary (~2006), never crossing to the
next partition. The opaque ContinuationToken/ContinuationHash echoed in every
response could NOT be fed back via any query-param or header form (all variants
returned page 0 unchanged). So global enumeration is impossible.

What DOES enumerate cleanly is per-competition: /calendar/matches?idCompetition=X
paged via `from=<max Date seen>` walks a single competition's matches all the way
to the present (verified to 2026) and terminates when no new IdMatch appears.
This is the strategy the handoff points at ("enumerate competitions via
/competitions, then iterate calendar/matches and standings per competition").

  * /competitions?count=1000 returns the full competition list in one page
    (522 observed; a ContinuationToken would signal growth past 1000 -> raise).
  * There is NO season/stage listing endpoint (bare /seasons is 401; the
    per-competition season paths return null). Season+stage ids are therefore
    discovered from the competition's own match records, which carry
    IdSeason/IdStage on every row. Standings are then fetched per
    (competition, season, stage); knockout stages return an empty Results set.

Fetch shape
-----------
Record-stream firehose, batched per competition (one raw NDJSON asset per
competition, asset id ``fifa-<entity>-<IdCompetition>``). Match/standing records
are deeply nested (Home/Away team objects, Officials arrays, localized Name
arrays, free-form Properties) so NDJSON is the right raw format, not parquet.

No incremental query filter exists (no since/modifiedAfter/ETag), so each refresh
re-pulls the full corpus: state tracks which competitions are done *within the
current cycle* and, once every competition is done, the next run resets and
re-pulls (picking up new matches and score revisions). State is also the in-run
resume point — a per-run wall-clock budget stops the crawl cleanly and the next
invocation continues, so a single huge run is never required.
"""

import time

import httpx
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
    save_raw_ndjson,
    save_state,
)

BASE = "https://api.fifa.com/api/v3"
LANG = "en"
PAGE = 500                       # max page size accepted (count>500 is capped to 500)
STATE_VERSION = 1

# Soft per-run budget: hitting it returns cleanly with state advanced so the next
# invocation resumes. This is deliberate pacing, expected to fire on slow standings
# crawls — NOT a safety ceiling.
MAX_FETCH_SECONDS = 900

# Safety ceiling on a single competition's match pagination. A competition needing
# >200 pages (=100k matches) means the source grew past expectation or pagination
# is looping — surface it loudly rather than truncate silently.
MAX_PAGES_PER_COMP = 200

SKIP_TTL_SECONDS = 14 * 86400

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
def _get_json(url: str, params: dict):
    """GET + parse JSON. Returns the decoded body, which may legitimately be
    ``None`` — the API emits a literal ``null`` body at end-of-data / partition
    boundaries. Callers treat a non-dict body as "no results"."""
    resp = get(url, params=params, timeout=(10.0, 120.0))
    resp.raise_for_status()
    return resp.json()


def _discover_competitions() -> list[str]:
    """Full competition id list in one page (count=1000). A ContinuationToken in
    the response means there are >1000 competitions — raise so the growth is
    handled explicitly rather than silently truncated."""
    body = _get_json(f"{BASE}/competitions", {"count": 1000, "language": LANG})
    if not isinstance(body, dict) or not body.get("Results"):
        raise RuntimeError("FIFA /competitions returned no Results")
    if body.get("ContinuationToken"):
        raise RuntimeError(
            "FIFA /competitions exceeded a single count=1000 page — add pagination"
        )
    return [c["IdCompetition"] for c in body["Results"] if c.get("IdCompetition")]


def _crawl_competition_matches(id_competition: str) -> list[dict]:
    """All match records for one competition, deduped by IdMatch.

    Pages /calendar/matches?idCompetition= ascending via ``from=<max Date seen>``.
    Terminates when a page yields no new IdMatch (the ascending cursor has caught
    up to the present) or the body goes null/empty.
    """
    seen: dict[str, dict] = {}
    frm: str | None = None
    last_max: str | None = None
    pages = 0

    while True:
        params = {"count": PAGE, "language": LANG, "idCompetition": id_competition}
        if frm:
            params["from"] = frm
        body = _get_json(f"{BASE}/calendar/matches", params)
        results = body.get("Results", []) if isinstance(body, dict) else []
        if not results:
            break

        pages += 1
        if pages > MAX_PAGES_PER_COMP:
            raise RuntimeError(
                f"competition {id_competition} exceeded {MAX_PAGES_PER_COMP} "
                f"match pages — source grew past expectation or cursor is looping"
            )

        new = 0
        page_max = None
        for m in results:
            mid = m.get("IdMatch")
            if mid is not None and mid not in seen:
                seen[mid] = m
                new += 1
            d = m.get("Date")
            if d and (page_max is None or d > page_max):
                page_max = d

        # Converged: this page added nothing, or the ascending date cursor can no
        # longer advance (all remaining matches share the boundary timestamp).
        if new == 0 or page_max is None or page_max == last_max:
            break
        last_max = page_max
        frm = page_max

    return list(seen.values())


def _fetch_standings_for_competition(id_competition: str) -> list[dict]:
    """Standing rows for every (season, stage) the competition's matches touch.

    Season/stage ids are discovered from the match records (no listing endpoint
    exists). Knockout stages return an empty Results envelope and contribute
    nothing; group/league stages return one row per team.
    """
    matches = _crawl_competition_matches(id_competition)
    tuples = sorted(
        {
            (m.get("IdSeason"), m.get("IdStage"))
            for m in matches
            if m.get("IdSeason") and m.get("IdStage")
        }
    )
    rows: list[dict] = []
    for season, stage in tuples:
        body = _get_json(
            f"{BASE}/calendar/{id_competition}/{season}/{stage}/Standing",
            {"language": LANG},
        )
        results = body.get("Results", []) if isinstance(body, dict) else []
        rows.extend(results)
    return rows


def _load_cycle_state(state_key: str, competitions: list[str]) -> tuple[set, dict]:
    """Load resume state, resetting on schema drift, expired skips, or a completed
    cycle (every competition done -> start a fresh full re-pull)."""
    state = load_state(state_key)
    if state.get("schema_version") != STATE_VERSION:
        state = {}

    now = int(time.time())
    skipped = {
        cid: info
        for cid, info in state.get("skipped", {}).items()
        if info.get("expires_at", 0) > now
    }

    done = set(state.get("done", []))
    comp_set = set(competitions)
    if done >= comp_set:  # previous cycle finished -> re-pull everything
        done = set()
        skipped = {}
    else:
        done &= comp_set  # drop competitions that have since disappeared

    return done, {"skipped": skipped}


def _run_per_competition(node_id: str, worker) -> None:
    """Shared firehose driver: walk the competition list, run ``worker`` per
    competition, write its batch (raw first), then advance state. Honours the
    per-run wall-clock budget and per-competition fault isolation."""
    state_key = node_id
    competitions = _discover_competitions()
    done, carry = _load_cycle_state(state_key, competitions)
    skipped = carry["skipped"]

    remaining = [c for c in competitions if c not in done]
    deadline = time.monotonic() + MAX_FETCH_SECONDS
    processed = 0
    records = 0

    def _persist():
        save_state(
            state_key,
            {
                "schema_version": STATE_VERSION,
                "done": sorted(done),
                "skipped": skipped,
                "competitions": len(competitions),
                "last_run_stats": {
                    "processed_this_run": processed,
                    "records_this_run": records,
                },
            },
        )

    for id_competition in remaining:
        if time.monotonic() > deadline:
            print(
                f"[{node_id}] soft budget ({MAX_FETCH_SECONDS}s) reached after "
                f"{processed} competitions; {len(remaining) - processed} remain "
                f"-> resuming next run",
                flush=True,
            )
            break

        asset = f"{node_id}-{id_competition}"
        try:
            rows = worker(id_competition)
        except httpx.HTTPError as exc:
            # Transient retries exhausted or a permanent 4xx — isolate to this
            # competition, mark it done so the cycle progresses, and retry it next
            # cycle. Programming errors (KeyError/TypeError/...) are NOT caught.
            url = getattr(getattr(exc, "request", None), "url", asset)
            print(
                f"[{node_id}] skip competition {id_competition}: "
                f"{type(exc).__name__} on {url}",
                flush=True,
            )
            skipped[id_competition] = {
                "reason": f"{type(exc).__name__}",
                "expires_at": int(time.time()) + SKIP_TTL_SECONDS,
            }
            done.add(id_competition)
            processed += 1
            _persist()
            continue

        if rows:
            save_raw_ndjson(rows, asset)  # raw FIRST
            records += len(rows)
        done.add(id_competition)  # then advance state
        processed += 1
        _persist()

        if processed % 25 == 0:
            print(
                f"[{node_id}] {processed}/{len(remaining)} competitions, "
                f"{records} records",
                flush=True,
            )

    cycle = "COMPLETE" if done >= set(competitions) else "partial"
    print(
        f"[{node_id}] run finished: {processed} competitions, {records} records, "
        f"cycle {cycle}",
        flush=True,
    )


def fetch_matches(node_id: str) -> None:
    _run_per_competition(node_id, _crawl_competition_matches)


def fetch_standings(node_id: str) -> None:
    _run_per_competition(node_id, _fetch_standings_for_competition)


DOWNLOAD_SPECS = [
    NodeSpec(id="fifa-matches", fn=fetch_matches, kind="download"),
    NodeSpec(id="fifa-standings", fn=fetch_standings, kind="download"),
]
