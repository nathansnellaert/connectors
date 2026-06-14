"""UN Population Division — World Population Prospects (WPP) 2024.

Mechanism: bulk_csv. The WPP 2024 "Standard projections" CSV export publishes
gzipped CSV files (one full table per request, no auth, no pagination) under
  https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/

Each entity in the union maps to one or more of these files. We download the
gzip bytes and store them verbatim as `*.csv.gz` raw assets — DuckDB reads
`.csv.gz` natively, so the SQL transforms do all parsing/typing. No incremental
support exists on this source (research handoff); WPP revisions land ~biennially
and bake the revision year into the file name (WPP2024_), so a new revision means
updating the file names below. Re-fetch is a full pull each refresh.

Scoping decisions (documented; the same mechanism exposes the rest if needed):
  * demographic-indicators — Medium + OtherVariants (full variant coverage).
  * fertility-by-age       — 5-year age groups (Age5). Single-year (Age1) is
                             ~300MB gz and omitted.
  * life-tables            — Abridged Medium, both time spans (1950-2023 +
                             2024-2100). Complete life tables not published as CSV.
  * migration              — net migration count/rate columns of the Demographic
                             Indicators (Medium) file; WPP ships no dedicated
                             migration CSV.
  * population-by-age-sex  — 5-year age groups, Medium, both 1-January and 1-July
                             reference dates (distinguished by an appended RefDate
                             column). Single-year-of-age files (~62MB gz each) and
                             OtherVariants (~231MB gz) are omitted.
"""

from __future__ import annotations

import zlib

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, raw_writer, save_raw_file

BASE = (
    "https://population.un.org/wpp/assets/Excel%20Files/"
    "1_Indicator%20(Standard)/CSV_FILES/"
)

# spec id -> list of (filename, batch_key). Stored verbatim as <id>-<key>.csv.gz.
FILES: dict[str, list[tuple[str, str]]] = {
    "un-population-division-demographic-indicators": [
        ("WPP2024_Demographic_Indicators_Medium.csv.gz", "medium"),
        ("WPP2024_Demographic_Indicators_OtherVariants.csv.gz", "othervariants"),
    ],
    "un-population-division-fertility-by-age": [
        ("WPP2024_Fertility_by_Age5.csv.gz", "age5"),
    ],
    "un-population-division-life-tables": [
        ("WPP2024_Life_Table_Abridged_Medium_1950-2023.csv.gz", "abridged-1950"),
        ("WPP2024_Life_Table_Abridged_Medium_2024-2100.csv.gz", "abridged-2024"),
    ],
    "un-population-division-migration": [
        ("WPP2024_Demographic_Indicators_Medium.csv.gz", "medium"),
    ],
}

# Population needs a RefDate column injected to distinguish 1-Jan from 1-July
# rows once DuckDB unions the two files. spec id -> (filename, refdate, batch_key).
POPULATION_FILES: dict[str, list[tuple[str, str, str]]] = {
    "un-population-division-population-by-age-sex": [
        ("WPP2024_PopulationByAge5GroupSex_Medium.csv.gz", "1July", "july"),
        ("WPP2024_Population1JanuaryByAge5GroupSex_Medium.csv.gz", "1January", "jan"),
    ],
}

_CHUNK = 1 << 20  # 1 MiB of compressed bytes fed to the decompressor at a time

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
def _fetch_bytes(url: str) -> bytes:
    # Static gzip files run large (the biggest is ~150MB), so allow a long read.
    resp = get(url, timeout=(30.0, 600.0))
    resp.raise_for_status()
    return resp.content


def fetch_simple(node_id: str) -> None:
    """Download each WPP gzip for this entity and store it verbatim as csv.gz."""
    total = 0
    for filename, key in FILES[node_id]:
        url = BASE + filename
        content = _fetch_bytes(url)
        asset = f"{node_id}-{key}"
        save_raw_file(content, asset, extension="csv.gz")
        total += len(content)
        print(f"[{node_id}] saved {asset}.csv.gz <- {filename} ({len(content)} bytes)", flush=True)
    print(f"[{node_id}] done, {total} compressed bytes", flush=True)


def fetch_population(node_id: str) -> None:
    """Like fetch_simple, but append a RefDate column so 1-Jan and 1-July rows
    stay distinguishable after DuckDB unions the two files. Streaming line work
    keeps memory bounded; appending at end-of-line is safe regardless of any
    quoted commas inside Location values."""
    for filename, refdate, key in POPULATION_FILES[node_id]:
        url = BASE + filename
        content = _fetch_bytes(url)
        asset = f"{node_id}-{key}"
        tag = ("," + refdate + "\n").encode("utf-8")
        dec = zlib.decompressobj(zlib.MAX_WBITS | 16)
        n_rows = 0
        with raw_writer(asset, "csv.gz", mode="wb", compression="gzip") as out:
            buf = b""
            header_done = False
            for i in range(0, len(content), _CHUNK):
                buf += dec.decompress(content[i : i + _CHUNK])
                lines = buf.split(b"\n")
                buf = lines.pop()  # trailing partial line carries to next chunk
                for ln in lines:
                    ln = ln.rstrip(b"\r")
                    if not header_done:
                        out.write(ln + b",RefDate\n")
                        header_done = True
                        continue
                    if not ln:
                        continue
                    out.write(ln + tag)
                    n_rows += 1
            buf += dec.flush()
            buf = buf.rstrip(b"\r\n")
            if buf:
                out.write(buf + tag)
                n_rows += 1
        print(f"[{node_id}] saved {asset}.csv.gz <- {filename} ({n_rows} rows)", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id="un-population-division-demographic-indicators", fn=fetch_simple, kind="download"),
    NodeSpec(id="un-population-division-fertility-by-age", fn=fetch_simple, kind="download"),
    NodeSpec(id="un-population-division-life-tables", fn=fetch_simple, kind="download"),
    NodeSpec(id="un-population-division-migration", fn=fetch_simple, kind="download"),
    NodeSpec(id="un-population-division-population-by-age-sex", fn=fetch_population, kind="download"),
]


# ---------------------------------------------------------------------------
# Transforms — one published Delta table per subset. The SQL is the parse/type
# gate; DuckDB infers types from the csv.gz and we cast/rename to a clean shape.
# ---------------------------------------------------------------------------

_DEMOGRAPHIC_SQL = '''
    SELECT
        CAST(LocID AS BIGINT)   AS location_id,
        ISO3_code               AS iso3_code,
        Location                AS location,
        Variant                 AS variant,
        CAST(Time AS INTEGER)   AS year,
        * EXCLUDE (SortOrder, LocID, Notes, ISO3_code, ISO2_code, SDMX_code,
                   LocTypeID, LocTypeName, ParentID, Location, VarID, Variant, Time)
    FROM "un-population-division-demographic-indicators"
    WHERE LocID IS NOT NULL AND Time IS NOT NULL
'''

_FERTILITY_SQL = '''
    SELECT
        CAST(LocID AS BIGINT)        AS location_id,
        ISO3_code                    AS iso3_code,
        Location                     AS location,
        Variant                      AS variant,
        CAST(Time AS INTEGER)        AS year,
        AgeGrp                       AS age_group,
        CAST(AgeGrpStart AS INTEGER) AS age_start,
        CAST(AgeGrpSpan AS INTEGER)  AS age_span,
        CAST(ASFR AS DOUBLE)         AS asfr,
        CAST(PASFR AS DOUBLE)        AS pasfr,
        CAST(Births AS DOUBLE)       AS births_thousands
    FROM "un-population-division-fertility-by-age"
    WHERE LocID IS NOT NULL AND Time IS NOT NULL AND AgeGrp IS NOT NULL
'''

# DuckDB de-dups the case-insensitive collision lx/Lx by renaming the second
# (Lx) to Lx_1; the lowercase lx keeps its name.
_LIFE_TABLES_SQL = '''
    SELECT
        CAST(LocID AS BIGINT)        AS location_id,
        ISO3_code                    AS iso3_code,
        Location                     AS location,
        Variant                      AS variant,
        CAST(Time AS INTEGER)        AS year,
        CAST(SexID AS INTEGER)       AS sex_id,
        Sex                          AS sex,
        AgeGrp                       AS age_group,
        CAST(AgeGrpStart AS INTEGER) AS age_start,
        CAST(AgeGrpSpan AS INTEGER)  AS age_span,
        CAST(mx AS DOUBLE)           AS mx,
        CAST(qx AS DOUBLE)           AS qx,
        CAST(px AS DOUBLE)           AS px,
        CAST(lx AS DOUBLE)           AS lx,
        CAST(dx AS DOUBLE)           AS dx,
        CAST("Lx_1" AS DOUBLE)       AS person_years_lx,
        CAST(Sx AS DOUBLE)           AS survival_ratio_sx,
        CAST(Tx AS DOUBLE)           AS total_person_years_tx,
        CAST(ex AS DOUBLE)           AS life_expectancy_ex,
        CAST(ax AS DOUBLE)           AS ax
    FROM "un-population-division-life-tables"
    WHERE LocID IS NOT NULL AND Time IS NOT NULL AND AgeGrp IS NOT NULL
'''

_MIGRATION_SQL = '''
    SELECT
        CAST(LocID AS BIGINT)         AS location_id,
        ISO3_code                     AS iso3_code,
        Location                      AS location,
        Variant                       AS variant,
        CAST(Time AS INTEGER)         AS year,
        CAST(NetMigrations AS DOUBLE) AS net_migrants_thousands,
        CAST(CNMR AS DOUBLE)          AS net_migration_rate
    FROM "un-population-division-migration"
    WHERE LocID IS NOT NULL AND Time IS NOT NULL
'''

_POPULATION_SQL = '''
    SELECT
        CAST(LocID AS BIGINT)        AS location_id,
        ISO3_code                    AS iso3_code,
        Location                     AS location,
        Variant                      AS variant,
        CAST(Time AS INTEGER)        AS year,
        RefDate                      AS reference_date,
        AgeGrp                       AS age_group,
        CAST(AgeGrpStart AS INTEGER) AS age_start,
        CAST(AgeGrpSpan AS INTEGER)  AS age_span,
        CAST(PopMale AS DOUBLE)      AS population_male_thousands,
        CAST(PopFemale AS DOUBLE)    AS population_female_thousands,
        CAST(PopTotal AS DOUBLE)     AS population_total_thousands
    FROM "un-population-division-population-by-age-sex"
    WHERE LocID IS NOT NULL AND Time IS NOT NULL AND AgeGrp IS NOT NULL
'''

TRANSFORM_SPECS = [
    SqlNodeSpec(
        id="un-population-division-demographic-indicators-transform",
        deps=["un-population-division-demographic-indicators"],
        sql=_DEMOGRAPHIC_SQL,
    ),
    SqlNodeSpec(
        id="un-population-division-fertility-by-age-transform",
        deps=["un-population-division-fertility-by-age"],
        sql=_FERTILITY_SQL,
    ),
    SqlNodeSpec(
        id="un-population-division-life-tables-transform",
        deps=["un-population-division-life-tables"],
        sql=_LIFE_TABLES_SQL,
    ),
    SqlNodeSpec(
        id="un-population-division-migration-transform",
        deps=["un-population-division-migration"],
        sql=_MIGRATION_SQL,
    ),
    SqlNodeSpec(
        id="un-population-division-population-by-age-sex-transform",
        deps=["un-population-division-population-by-age-sex"],
        sql=_POPULATION_SQL,
    ),
]
