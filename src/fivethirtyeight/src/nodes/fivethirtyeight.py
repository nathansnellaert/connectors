"""FiveThirtyEight download node — bulk CSV fetch from the open-data repo.

Mechanism (from research, id=bulk_csv): every dataset is a single CSV served in
full from a persistent raw.githubusercontent.com URL, no auth, no pagination:

    https://raw.githubusercontent.com/fivethirtyeight/data/master/<path>

Each entity in the union maps 1:1 to one CSV file in the repo. The repo files
are small persistent artifacts (a few KB to a few MB each) that are re-published
in place, so the right shape is a **stateless full re-pull**: fetch the whole
CSV every run and overwrite. No watermark, no cursor — revisions/corrections to
a file are picked up for free because we never trust a stored high-water mark.

Raw is stored as the **CSV bytes verbatim** via save_raw_file. Each of the 37
datasets carries its own distinct, file-specific column schema (columns vary
widely across files, some carry a UTF-8 BOM), so there is no single tabular
schema to impose here. Faithful byte storage defers all typing to the transform
step, where each file gets parsed against its own header.

Freshness gating is the maintain step's job — if a fetch fn is invoked, it
fetches.
"""
import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, get, save_raw_file

_SLUG = "fivethirtyeight"
_BASE = "https://raw.githubusercontent.com/fivethirtyeight/data/master/"

# entity_id (verbatim from the entity union) -> repo-relative path. Copied from
# the collect catalog (source_metadata.path) for exactly the 37 union entities.
# The entity_id is a slug of the path; the path preserves the source's real
# casing (e.g. RAPTOR, DISTRICTS) and directory structure, so we keep it verbatim.
ENTITY_PATHS = {
    "airline-safety-airline-safety": "airline-safety/airline-safety.csv",
    "alcohol-consumption-drinks": "alcohol-consumption/drinks.csv",
    "births-us_births_1994-2003_cdc_nchs": "births/US_births_1994-2003_CDC_NCHS.csv",
    "births-us_births_2000-2014_ssa": "births/US_births_2000-2014_SSA.csv",
    "college-majors-all-ages": "college-majors/all-ages.csv",
    "college-majors-grad-students": "college-majors/grad-students.csv",
    "college-majors-recent-grads": "college-majors/recent-grads.csv",
    "congress-age-congress-terms": "congress-age/congress-terms.csv",
    "congress-demographics-data_aging_congress": "congress-demographics/data_aging_congress.csv",
    "congress-generic-ballot-generic_topline_historical": "congress-generic-ballot/generic_topline_historical.csv",
    "drug-use-by-age-drug-use-by-age": "drug-use-by-age/drug-use-by-age.csv",
    "hate-crimes-hate_crimes": "hate-crimes/hate_crimes.csv",
    "marriage-both_sexes": "marriage/both_sexes.csv",
    "nba-elo-nbaallelo": "nba-elo/nbaallelo.csv",
    "nba-raptor-historical_raptor_by_player": "nba-raptor/historical_RAPTOR_by_player.csv",
    "nba-raptor-historical_raptor_by_team": "nba-raptor/historical_RAPTOR_by_team.csv",
    "nba-raptor-modern_raptor_by_player": "nba-raptor/modern_RAPTOR_by_player.csv",
    "nba-raptor-modern_raptor_by_team": "nba-raptor/modern_RAPTOR_by_team.csv",
    "partisan-lean-2018-fivethirtyeight_partisan_lean_districts": "partisan-lean/2018/fivethirtyeight_partisan_lean_DISTRICTS.csv",
    "partisan-lean-2018-fivethirtyeight_partisan_lean_states": "partisan-lean/2018/fivethirtyeight_partisan_lean_STATES.csv",
    "partisan-lean-2020-fivethirtyeight_partisan_lean_districts": "partisan-lean/2020/fivethirtyeight_partisan_lean_DISTRICTS.csv",
    "partisan-lean-2020-fivethirtyeight_partisan_lean_states": "partisan-lean/2020/fivethirtyeight_partisan_lean_STATES.csv",
    "partisan-lean-2021-fivethirtyeight_partisan_lean_districts": "partisan-lean/2021/fivethirtyeight_partisan_lean_DISTRICTS.csv",
    "partisan-lean-2021-fivethirtyeight_partisan_lean_states": "partisan-lean/2021/fivethirtyeight_partisan_lean_STATES.csv",
    "partisan-lean-fivethirtyeight_partisan_lean_districts": "partisan-lean/fivethirtyeight_partisan_lean_DISTRICTS.csv",
    "partisan-lean-fivethirtyeight_partisan_lean_states": "partisan-lean/fivethirtyeight_partisan_lean_STATES.csv",
    "police-killings-police_killings": "police-killings/police_killings.csv",
    "polls-pres_pollaverages_1968-2016": "polls/pres_pollaverages_1968-2016.csv",
    "polls-pres_primary_avgs_1980-2016": "polls/pres_primary_avgs_1980-2016.csv",
    "pollster-ratings-2016-pollster-ratings": "pollster-ratings/2016/pollster-ratings.csv",
    "pollster-ratings-2018-pollster-ratings": "pollster-ratings/2018/pollster-ratings.csv",
    "pollster-ratings-2019-pollster-ratings": "pollster-ratings/2019/pollster-ratings.csv",
    "pollster-ratings-2020-pollster-ratings": "pollster-ratings/2020/pollster-ratings.csv",
    "pollster-ratings-2021-pollster-ratings": "pollster-ratings/2021/pollster-ratings.csv",
    "pollster-ratings-2023-pollster-ratings": "pollster-ratings/2023/pollster-ratings.csv",
    "pollster-ratings-pollster-ratings-combined": "pollster-ratings/pollster-ratings-combined.csv",
    "pollster-ratings-raw_polls": "pollster-ratings/raw_polls.csv",
}

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
def _fetch_csv(url: str) -> bytes:
    resp = get(url, timeout=(10.0, 120.0))
    # A removed/renamed file (4xx) is permanent and not retried; it raises here
    # and fails this one spec loudly, which is the right signal for a URL that
    # research verified as persistent. Sibling specs run in their own processes.
    resp.raise_for_status()
    return resp.content


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    entity_id = node_id[len(_SLUG) + 1:]  # strip "fivethirtyeight-" prefix
    path = ENTITY_PATHS[entity_id]  # KeyError = bug (spec set out of sync); let it raise
    url = _BASE + path

    content = _fetch_csv(url)
    if not content:
        raise AssertionError(f"{asset}: empty CSV body from {url}")

    save_raw_file(content, asset, extension="csv")
    print(f"{asset}: fetched {len(content)} bytes from {path}", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"{_SLUG}-{entity_id}",
        fn=fetch_one,
        kind="download",
    )
    for entity_id in ENTITY_PATHS
]
