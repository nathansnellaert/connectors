"""UNHCR Refugee Statistics connector.

Mechanism: bulk_csv (research-chosen). Each statistical endpoint under
https://api.unhcr.org/population/v1/ supports `download=true`, which switches
it from paginated JSON to a single ZIP-of-CSV bulk export covering the full
filtered set. We request the full origin x asylum breakdown (coo_all/coa_all),
ISO3 country codes (cf_type=ISO), and the full year span discovered from the
`years/` reference endpoint.

Fetch shape: stateless full re-pull. The whole corpus is a handful of MB per
endpoint and the source has no incremental filter (no since/cursor — only
yearFrom/yearTo), so we re-fetch everything each run and overwrite. The source
updates annually (~mid-year); revisions and late corrections are picked up for
free because no watermark is trusted.

Raw is saved as NDJSON of all-string values (CSV cells, with ""/"-" normalised
to null): the per-endpoint measure columns differ, so a single schema does not
fit and NDJSON re-types cleanly in the transform SQL. CSV headers are
human-readable ("Refugees under UNHCR's mandate"); we slugify them
deterministically so the SQL references stable column names.
"""

import csv as _csv
import io
import re
import zipfile

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, save_raw_ndjson

BASE = "https://api.unhcr.org/population/v1"

# The entity union — one statistical endpoint per entry (copied from
# data/sources/unhcr/steps/.../entity_union.json). Each id is also the bulk
# endpoint path segment and the name of the CSV inside its ZIP.
ENTITY_IDS = [
    "asylum-applications",
    "asylum-decisions",
    "demographics",
    "idmc",
    "nowcasting",
    "population",
    "solutions",
    "unrwa",
]

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
def _get(url: str, **kwargs) -> httpx.Response:
    resp = get(url, timeout=(10.0, 180.0), **kwargs)
    resp.raise_for_status()
    return resp


def _slug(header: str) -> str:
    """Deterministically turn a human CSV header into a stable column name.

    Must stay in sync with the column names referenced in TRANSFORM_SPECS.
    """
    s = header.strip().lower().replace("'", "")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def _discover_year_range() -> tuple[int, int]:
    """Full year span available across the corpus, from the reference endpoint.

    Avoids hardcoding a literal range: nowcasting carries forward-looking years
    (e.g. 2026) and the historical floor is 1951, so a static bound would silently
    drop rows as the source advances.
    """
    resp = _get(f"{BASE}/years/", params={"limit": 1000})
    items = resp.json().get("items", [])
    years = [it["year"] for it in items if it.get("year") is not None]
    if not years:
        raise AssertionError("years/ reference endpoint returned no years")
    return min(years), max(years)


def fetch_one(node_id: str) -> None:
    asset = node_id  # the runtime passes the spec id; it IS the asset name
    endpoint = node_id[len("unhcr-"):]

    year_from, year_to = _discover_year_range()
    params = {
        "coo_all": "true",
        "coa_all": "true",
        "download": "true",
        "cf_type": "ISO",
        "yearFrom": year_from,
        "yearTo": year_to,
    }
    resp = _get(f"{BASE}/{endpoint}/", params=params)

    content = resp.content
    if content[:2] != b"PK":
        raise AssertionError(
            f"{endpoint}: expected ZIP, got "
            f"{resp.headers.get('content-type')} ({len(content)} bytes)"
        )

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        csv_name = f"{endpoint}.csv"
        if csv_name not in z.namelist():
            raise AssertionError(
                f"{endpoint}: {csv_name} not in ZIP ({z.namelist()})"
            )
        text = z.read(csv_name).decode("utf-8")

    reader = _csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise AssertionError(f"{endpoint}: empty CSV (no header)")
    keys = [_slug(h) for h in header]

    rows = []
    for rec in reader:
        d = {}
        for k, v in zip(keys, rec):
            v = v.strip()
            d[k] = None if v in ("", "-") else v
        rows.append(d)

    if not rows:
        raise AssertionError(f"{endpoint}: 0 data rows in {csv_name}")

    save_raw_ndjson(rows, asset)


DOWNLOAD_SPECS = [
    NodeSpec(
        id=f"unhcr-{eid.lower().replace('_', '-')}",
        fn=fetch_one,
        kind="download",
    )
    for eid in ENTITY_IDS
]


# --- Transforms: one published Delta table per endpoint -------------------
# All raw values are strings (or null); TRY_CAST coerces measure columns and
# leaves unparseable cells null instead of failing the whole node. The shared
# dimension columns (year + origin/asylum name & ISO3) are present on every
# endpoint; only the measure columns differ.

_DIMS = '''
    TRY_CAST(year AS INTEGER)        AS year,
    country_of_origin                AS country_of_origin,
    country_of_origin_iso            AS country_of_origin_iso,
    country_of_asylum                AS country_of_asylum,
    country_of_asylum_iso            AS country_of_asylum_iso'''

_SQL = {
    "asylum-applications": f'''
        SELECT{_DIMS},
            authority                                  AS authority,
            application_type                           AS application_type,
            stage_of_procedure                         AS stage_of_procedure,
            cases_persons                              AS cases_or_persons,
            TRY_CAST(applied AS BIGINT)                AS applied
        FROM "unhcr-asylum-applications"
        WHERE year IS NOT NULL
    ''',
    "asylum-decisions": f'''
        SELECT{_DIMS},
            authority                                  AS authority,
            stage_of_procedure                         AS stage_of_procedure,
            cases_persons                              AS cases_or_persons,
            TRY_CAST(recognized_decisions AS BIGINT)   AS recognized_decisions,
            TRY_CAST(complementary_protection AS BIGINT) AS complementary_protection,
            TRY_CAST(rejected_decisions AS BIGINT)     AS rejected_decisions,
            TRY_CAST(otherwise_closed AS BIGINT)       AS otherwise_closed,
            TRY_CAST(total_decisions AS BIGINT)        AS total_decisions
        FROM "unhcr-asylum-decisions"
        WHERE year IS NOT NULL
    ''',
    "demographics": f'''
        SELECT{_DIMS},
            TRY_CAST(female_0_4 AS BIGINT)    AS female_0_4,
            TRY_CAST(female_5_11 AS BIGINT)   AS female_5_11,
            TRY_CAST(female_12_17 AS BIGINT)  AS female_12_17,
            TRY_CAST(female_18_59 AS BIGINT)  AS female_18_59,
            TRY_CAST(female_60 AS BIGINT)     AS female_60_plus,
            TRY_CAST(female_other AS BIGINT)  AS female_other,
            TRY_CAST(female_total AS BIGINT)  AS female_total,
            TRY_CAST(male_0_4 AS BIGINT)      AS male_0_4,
            TRY_CAST(male_5_11 AS BIGINT)     AS male_5_11,
            TRY_CAST(male_12_17 AS BIGINT)    AS male_12_17,
            TRY_CAST(male_18_59 AS BIGINT)    AS male_18_59,
            TRY_CAST(male_60 AS BIGINT)       AS male_60_plus,
            TRY_CAST(male_other AS BIGINT)    AS male_other,
            TRY_CAST(male_total AS BIGINT)    AS male_total,
            TRY_CAST(total AS BIGINT)         AS total
        FROM "unhcr-demographics"
        WHERE year IS NOT NULL
    ''',
    "idmc": f'''
        SELECT{_DIMS},
            TRY_CAST(total AS BIGINT)         AS total
        FROM "unhcr-idmc"
        WHERE year IS NOT NULL
    ''',
    "nowcasting": f'''
        SELECT{_DIMS},
            month                                            AS month,
            source                                           AS source,
            TRY_CAST(refugees_under_unhcrs_mandate AS BIGINT) AS refugees,
            TRY_CAST(asylum_seekers AS BIGINT)               AS asylum_seekers
        FROM "unhcr-nowcasting"
        WHERE year IS NOT NULL
    ''',
    "population": f'''
        SELECT{_DIMS},
            TRY_CAST(refugees_under_unhcrs_mandate AS BIGINT) AS refugees,
            TRY_CAST(asylum_seekers AS BIGINT)               AS asylum_seekers,
            TRY_CAST(returned_refugees AS BIGINT)            AS returned_refugees,
            TRY_CAST(idps_of_concern_to_unhcr AS BIGINT)     AS idps,
            TRY_CAST(returned_idpss AS BIGINT)               AS returned_idps,
            TRY_CAST(stateless_persons AS BIGINT)            AS stateless_persons,
            TRY_CAST(others_of_concern AS BIGINT)            AS others_of_concern,
            TRY_CAST(other_people_in_need_of_international_protection AS BIGINT)
                                                             AS other_in_need_of_protection,
            TRY_CAST(host_community AS BIGINT)               AS host_community
        FROM "unhcr-population"
        WHERE year IS NOT NULL
    ''',
    "solutions": f'''
        SELECT{_DIMS},
            TRY_CAST(returned_refugees AS BIGINT)      AS returned_refugees,
            TRY_CAST(resettlement_arrivals AS BIGINT)  AS resettlement_arrivals,
            TRY_CAST(naturalisation AS BIGINT)         AS naturalisation,
            TRY_CAST(returned_idpss AS BIGINT)         AS returned_idps
        FROM "unhcr-solutions"
        WHERE year IS NOT NULL
    ''',
    "unrwa": f'''
        SELECT{_DIMS},
            TRY_CAST(total AS BIGINT)         AS total
        FROM "unhcr-unrwa"
        WHERE year IS NOT NULL
    ''',
}

TRANSFORM_SPECS = [
    SqlNodeSpec(
        id=f"unhcr-{eid}-transform",
        deps=[f"unhcr-{eid}"],
        sql=_SQL[eid],
    )
    for eid in ENTITY_IDS
]
