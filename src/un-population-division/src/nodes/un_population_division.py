"""UN Population Division — World Population Prospects (WPP) 2024.

Mechanism: bulk_csv. The WPP 2024 "Standard projections" CSV export publishes
gzipped CSV files (one full table per request, no auth, no pagination) under
  https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/

Each entity in the union maps to one or more of these files. We stream the gzip
through pyarrow's CSV reader (which honours the `"`-quoted Location values that
contain commas — DuckDB's auto-CSV quote detection does not, reliably) and write
a typed Parquet raw asset. Column types are pinned from the header so multi-file
assets and re-runs share an identical schema. The SQL transforms then cast/rename
to a clean published shape.

No incremental support exists on this source (research handoff); WPP revisions
land ~biennially and bake the revision year into the file name (WPP2024_), so a
new revision means updating the file names in SOURCES. Re-fetch is a full pull
each refresh; freshness gating is the maintain step's job.

Scoping decisions (the same mechanism exposes the rest if ever needed):
  * demographic-indicators — Medium + OtherVariants (full variant coverage).
  * fertility-by-age       — 5-year age groups (Age5). Single-year (Age1) is
                             ~300MB gz and omitted.
  * life-tables            — Abridged Medium, both time spans (1950-2023 +
                             2024-2100). Complete life tables aren't published as CSV.
  * migration              — net migration count/rate columns of the Demographic
                             Indicators (Medium) file; WPP ships no dedicated
                             migration CSV.
  * population-by-age-sex  — 5-year age groups, Medium, both 1-January and 1-July
                             reference dates (kept apart by an injected RefDate
                             column). Single-year-of-age (~62MB gz each) and
                             OtherVariants (~231MB gz) are omitted.
"""

from __future__ import annotations

import zlib

import httpx
import pyarrow as pa
import pyarrow.csv as pacsv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from subsets_utils import NodeSpec, SqlNodeSpec, get, raw_parquet_writer

BASE = (
    "https://population.un.org/wpp/assets/Excel%20Files/"
    "1_Indicator%20(Standard)/CSV_FILES/"
)

# spec id -> list of (filename, refdate). refdate is None for files that don't
# need a reference-date marker; for population it tags 1-Jan vs 1-July rows so
# they stay distinct after the two files are unioned.
SOURCES: dict[str, list[tuple[str, str | None]]] = {
    "un-population-division-demographic-indicators": [
        ("WPP2024_Demographic_Indicators_Medium.csv.gz", None),
        ("WPP2024_Demographic_Indicators_OtherVariants.csv.gz", None),
    ],
    "un-population-division-fertility-by-age": [
        ("WPP2024_Fertility_by_Age5.csv.gz", None),
    ],
    "un-population-division-life-tables": [
        ("WPP2024_Life_Table_Abridged_Medium_1950-2023.csv.gz", None),
        ("WPP2024_Life_Table_Abridged_Medium_2024-2100.csv.gz", None),
    ],
    "un-population-division-migration": [
        ("WPP2024_Demographic_Indicators_Medium.csv.gz", None),
    ],
    "un-population-division-population-by-age-sex": [
        ("WPP2024_PopulationByAge5GroupSex_Medium.csv.gz", "1July"),
        ("WPP2024_Population1JanuaryByAge5GroupSex_Medium.csv.gz", "1January"),
    ],
}

# Column-name -> pinned arrow type. Anything not listed is a numeric measure and
# is read as float64, so the schema is identical across files and reruns.
_STRING_COLS = {
    "Notes", "ISO3_code", "ISO2_code", "SDMX_code", "LocTypeName",
    "Location", "Variant", "Sex", "AgeGrp", "RefDate",
}
_INT_COLS = {
    "SortOrder", "LocID", "LocTypeID", "ParentID", "VarID", "Time",
    "SexID", "AgeGrpStart", "AgeGrpSpan",
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
def _fetch_bytes(url: str) -> bytes:
    # Static gzip files run large (the biggest is ~150MB), so allow a long read.
    resp = get(url, timeout=(30.0, 600.0))
    resp.raise_for_status()
    return resp.content


def _header_cols(content: bytes) -> list[str]:
    """Decompress just enough of the gzip to read the CSV header row."""
    dec = zlib.decompressobj(zlib.MAX_WBITS | 16)
    prefix = dec.decompress(content[:65536])
    line = prefix.split(b"\n", 1)[0].decode("utf-8-sig").rstrip("\r")
    return line.split(",")


def _column_types(cols: list[str]) -> dict[str, pa.DataType]:
    types: dict[str, pa.DataType] = {}
    for c in cols:
        if c in _STRING_COLS:
            types[c] = pa.string()
        elif c in _INT_COLS:
            types[c] = pa.int64()
        else:
            types[c] = pa.float64()
    return types


def _dedup_names(names: list[str]) -> list[str]:
    """Rename case-insensitive duplicate columns (WPP life tables have both
    `lx` and `Lx`) the same way DuckDB would: append `_1`, `_2`, ... to the
    later collision so each column is addressable in SQL."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        low = n.lower()
        if low in seen:
            seen[low] += 1
            out.append(f"{n}_{seen[low]}")
        else:
            seen[low] = 0
            out.append(n)
    return out


def _iter_tables(sources: list[tuple[str, str | None]]):
    """Yield pyarrow Tables (one per CSV block) across all source files of an
    asset, with columns deduped and an optional RefDate column appended."""
    base_names: list[str] | None = None
    for filename, refdate in sources:
        content = _fetch_bytes(BASE + filename)
        col_types = _column_types(_header_cols(content))
        src = pa.input_stream(pa.py_buffer(content), compression="gzip")
        reader = pacsv.open_csv(
            src,
            read_options=pacsv.ReadOptions(block_size=8 << 20),
            convert_options=pacsv.ConvertOptions(
                column_types=col_types,
                strings_can_be_null=True,
                null_values=[""],
            ),
        )
        names = _dedup_names([f.name for f in reader.schema])
        if base_names is None:
            base_names = names
        elif names != base_names:
            raise ValueError(
                f"column mismatch in {filename}: {names} != {base_names}"
            )
        for batch in reader:
            tbl = pa.Table.from_batches([batch.rename_columns(names)])
            if refdate is not None:
                tbl = tbl.append_column(
                    "RefDate", pa.array([refdate] * tbl.num_rows, type=pa.string())
                )
            yield tbl
        print(f"  parsed {filename}", flush=True)


def fetch_one(node_id: str) -> None:
    """Stream every WPP CSV for this entity into one typed Parquet raw asset."""
    gen = _iter_tables(SOURCES[node_id])
    try:
        first = next(gen)
    except StopIteration:
        raise ValueError(f"{node_id}: no rows parsed from source files")
    out_schema = first.schema
    total = 0
    with raw_parquet_writer(node_id, out_schema) as writer:
        writer.write_table(first)
        total += first.num_rows
        for tbl in gen:
            writer.write_table(tbl.cast(out_schema))
            total += tbl.num_rows
    print(f"[{node_id}] wrote {total} rows", flush=True)


DOWNLOAD_SPECS = [
    NodeSpec(id=node_id, fn=fetch_one, kind="download") for node_id in SOURCES
]


# ---------------------------------------------------------------------------
# Transforms — one published Delta table per subset. Raw is already typed
# Parquet; the SQL casts/renames to a clean published shape and drops null keys.
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

# `Lx` was renamed to `Lx_1` by _dedup_names (it collides with `lx`).
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
