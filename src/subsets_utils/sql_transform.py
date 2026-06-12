"""Runtime executor for SqlNodeSpec: raw asset(s) in → SQL → one Delta table.

The orchestrator routes SqlNodeSpec nodes here instead of calling a
connector-authored fn. `run_sql_node` is a top-level function so it survives
spawn-context pickling; the spec instance itself (id, deps, sql) is the only
payload.

Per dep, raw files are discovered by globbing the run-scoped raw dir
(`<dep>.<ext>` first, falling back to the batch layout `<dep>-*`), mapped to
the matching DuckDB reader by extension, and registered as a temp view named
after the dep id. The connector's SQL then runs against those views and the
result streams into `overwrite(<table>)`. Zero rows is a failure: the
transform is the correctness gate on raw, and an empty publish means the raw
shape or the SQL is wrong.
"""

from __future__ import annotations

import duckdb

from .config import raw_uri
from .delta import overwrite
from .duckdb import _configure
from .io import list_raw_files
from .spec import SqlNodeSpec

# Extension (after stripping compression suffixes) → DuckDB table function.
# DuckDB infers compression from the .zst/.gz suffix itself, so the reader
# gets the full path; we only strip suffixes to decide WHICH reader.
_COMPRESSION_SUFFIXES = (".zst", ".gz", ".bz2", ".xz")
_READERS = {
    ".parquet": "read_parquet",
    ".ndjson": "read_json_auto",
    ".jsonl": "read_json_auto",
    ".json": "read_json_auto",
    ".csv": "read_csv_auto",
    ".tsv": "read_csv_auto",
}


def _reader_for(rel_path: str) -> str | None:
    """Map a raw file's name to its DuckDB reader, or None if unreadable."""
    name = rel_path.lower()
    for suffix in _COMPRESSION_SUFFIXES:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    for ext, reader in _READERS.items():
        if name.endswith(ext):
            return reader
    return None


def _raw_base_uri() -> str:
    """The run-scoped raw dir (s3:// or local), derived the same way
    list_raw_files derives it — by probing raw_uri."""
    return raw_uri("__probe__", "__").rsplit("/", 1)[0]


def _read_clause(dep_id: str) -> tuple[list[str], str]:
    """Build the DuckDB FROM-clause for one dep's raw file(s).
    Returns (relative raw paths, FROM-clause) so the caller can record reads.

    Exact-name match (`<dep>.<ext>`) wins; only when absent do we accept the
    batch layout (`<dep>-*`) so a sibling asset whose id extends this one
    (e.g. `slug-us` vs `slug-us-east`) can't be swallowed by the glob.
    """
    rels = list_raw_files(f"{dep_id}.*")
    if not rels:
        rels = list_raw_files(f"{dep_id}-*")
    if not rels:
        raise FileNotFoundError(
            f"SqlNodeSpec dep {dep_id!r}: no raw files found "
            f"(globbed '{dep_id}.*' and '{dep_id}-*')"
        )

    readers = {r for r in (_reader_for(rel) for rel in rels) if r}
    unreadable = [rel for rel in rels if _reader_for(rel) is None]
    if unreadable:
        raise ValueError(
            f"SqlNodeSpec dep {dep_id!r}: raw file(s) not SQL-readable "
            f"{unreadable[:5]} — downloads feeding a SQL transform must write "
            "parquet/ndjson/csv (compressed ok); normalize in the download fn"
        )
    if len(readers) > 1:
        raise ValueError(
            f"SqlNodeSpec dep {dep_id!r}: mixed raw formats {sorted(readers)} "
            f"across {len(rels)} files — one format per asset"
        )

    base = _raw_base_uri()
    paths = [f"{base}/{rel}" for rel in rels]
    return rels, f"{readers.pop()}({paths})"


def run_sql_node(spec: SqlNodeSpec) -> None:
    """Execute one SqlNodeSpec: views for deps, run sql, overwrite Delta table."""
    _configure()  # S3 credentials for DuckDB when in cloud mode

    from .tracking import record_read

    for dep in spec.deps:
        rels, clause = _read_clause(dep)
        duckdb.sql(f'CREATE OR REPLACE TEMP VIEW "{dep}" AS SELECT * FROM {clause}')
        # DuckDB reads bypass load_raw_* so lineage is recorded here.
        for rel in rels:
            record_read(f"raw/{rel}")
        print(f'[sql-transform] view "{dep}" <- {clause[:200]}')

    # fetch_record_batch() streams — the result never fully materializes in
    # memory. Peek until the first non-empty batch BEFORE writing: an empty
    # result must fail without touching (and possibly clobbering) an existing
    # good table.
    reader = duckdb.sql(spec.sql).fetch_record_batch()
    peeked = []
    for batch in reader:
        peeked.append(batch)
        if batch.num_rows:
            break
    if not any(b.num_rows for b in peeked):
        raise ValueError(
            f"SqlNodeSpec {spec.id!r}: query produced 0 rows — transform is the "
            "correctness gate, an empty publish means raw or SQL is wrong"
        )

    def _chain():
        yield from peeked
        yield from reader

    import pyarrow as pa
    overwrite(pa.RecordBatchReader.from_batches(reader.schema, _chain()), spec.table)
