"""Delta table operations: merge, overwrite, append.

Simple, explicit API for writing to Delta tables.
No hidden defaults. No escape hatches.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable, CommitProperties
try:
    from deltalake.exceptions import TableNotFoundError
except ImportError:
    try:
        from deltalake import TableNotFoundError  # older deltalake
    except ImportError:
        TableNotFoundError = None  # fallback: we'll handle by exception type name

from .config import get_data_dir, is_cloud, get_storage_options, subsets_uri
from . import debug
from .io import data_hash
from .tracking import record_write


def _is_table_not_found(exc: Exception) -> bool:
    """Distinguish 'table does not exist yet' from other Delta errors."""
    if TableNotFoundError is not None and isinstance(exc, TableNotFoundError):
        return True
    # Fallback string match for deltalake versions without a typed exception
    msg = str(exc).lower()
    return "not a delta table" in msg or "no such file" in msg or "does not exist" in msg


def _run_commit_properties() -> CommitProperties | None:
    """Build CommitProperties with run context for Delta commits.

    Embeds run_id, connector name, and GitHub Actions metadata into the
    Delta Lake commitInfo so the server can extract lineage from the log.
    Returns None if no run context is available (e.g. ad-hoc local writes).
    """
    meta = {}
    for env_key, meta_key in [
        ("RUN_ID", "subsets.run_id"),
        ("CONNECTOR_NAME", "subsets.connector"),
        ("GITHUB_RUN_ID", "subsets.github_run_id"),
    ]:
        val = os.environ.get(env_key)
        if val:
            meta[meta_key] = val

    gh_run_id = os.environ.get("GITHUB_RUN_ID")
    gh_repo = os.environ.get("GITHUB_REPOSITORY")
    if gh_run_id and gh_repo:
        meta["subsets.github_run_url"] = (
            f"https://github.com/{gh_repo}/actions/runs/{gh_run_id}"
        )

    gh_sha = os.environ.get("GITHUB_SHA")
    if gh_sha:
        meta["subsets.git_commit"] = gh_sha

    return CommitProperties(custom_metadata=meta) if meta else None


@dataclass
class WriteResult:
    uri: str
    version: int
    hash: str
    rows: int


def validate_asset(
    name: str,
    *,
    key: Union[str, list[str]] = None,
    expected_columns: list[str] = None
) -> dict:
    """Validate an existing Delta table asset.

    Returns a report with:
    - row_count: number of rows
    - columns: list of columns
    - key_duplicates: number of duplicate key combinations (if key provided)
    - key_nulls: nulls per key column (if key provided)
    - needs_cleanup: True if issues found

    Args:
        name: Dataset name
        key: Expected key column(s) for uniqueness check
        expected_columns: Columns that should exist

    Returns:
        Validation report dict

    Raises:
        FileNotFoundError: If asset doesn't exist
    """
    uri = _get_uri(name)
    opts = _get_opts()

    try:
        dt = DeltaTable(uri, storage_options=opts)
        table = dt.to_pyarrow_table()
    except Exception as e:
        raise FileNotFoundError(f"Asset '{name}' not found: {e}")

    report = {
        "name": name,
        "row_count": len(table),
        "columns": table.column_names,
        "issues": [],
        "needs_cleanup": False
    }

    # Check expected columns
    if expected_columns:
        missing = set(expected_columns) - set(table.column_names)
        if missing:
            report["issues"].append(f"Missing columns: {missing}")
            report["needs_cleanup"] = True

    # Check key uniqueness and nulls
    if key:
        keys = [key] if isinstance(key, str) else key
        report["key"] = keys

        # Check key columns exist
        missing_keys = [k for k in keys if k not in table.column_names]
        if missing_keys:
            report["issues"].append(f"Key columns missing: {missing_keys}")
            report["needs_cleanup"] = True
        else:
            # Check nulls in key columns
            key_nulls = {}
            for k in keys:
                null_count = table[k].null_count
                if null_count > 0:
                    key_nulls[k] = null_count
            if key_nulls:
                report["key_nulls"] = key_nulls
                report["issues"].append(f"Nulls in key columns: {key_nulls}")
                report["needs_cleanup"] = True

            # Check duplicates
            import pyarrow.compute as pc
            if len(keys) == 1:
                unique_count = len(table[keys[0]].unique())
            else:
                combined = pc.binary_join_element_wise(
                    *[pc.cast(table[k], pa.string()) for k in keys],
                    "||"
                )
                unique_count = len(combined.unique())

            dup_count = len(table) - unique_count
            if dup_count > 0:
                report["key_duplicates"] = dup_count
                report["issues"].append(f"{dup_count} duplicate key combinations")
                report["needs_cleanup"] = True

    # Summary
    if report["needs_cleanup"]:
        print(f"⚠️  [{name}] Needs cleanup: {', '.join(report['issues'])}")
    else:
        print(f"✅ [{name}] Valid: {len(table):,} rows, key={key}")

    return report


def _get_uri(name: str) -> str:
    """Get Delta table URI based on environment."""
    if is_cloud():
        return subsets_uri(name)
    return str(Path(get_data_dir()) / "subsets" / name)


def _get_opts() -> dict | None:
    """Get storage options for cloud, None for local."""
    return get_storage_options() if is_cloud() else None


def _target_row_count(dt: DeltaTable) -> int:
    """Sum num_records from the Delta log's add actions.

    Reads only file-level metadata (parquet footer stats already in the log),
    no data scan. Returns -1 if unavailable so callers can still report.
    """
    try:
        # deltalake ≥1.0 returns an arro3 RecordBatch; bridge to pyarrow.
        batch = pa.record_batch(dt.get_add_actions(flatten=True))
        col = batch.column("num_records")
        return int(sum(v for v in col.to_pylist() if v is not None))
    except Exception:
        return -1


def _log_write(name: str, table: pa.Table, mode: str):
    """Log the write operation."""
    size_mb = round(table.nbytes / 1024 / 1024, 2)
    cols = ', '.join([f.name for f in table.schema])
    print(f"[{mode}] {name}: {len(table):,} rows, {len(table.schema)} cols ({cols}), {size_mb} MB")

    # Debug logging
    null_counts = {col: table[col].null_count for col in table.column_names if table[col].null_count > 0}
    debug.log_data_output(
        dataset_name=name,
        row_count=len(table),
        size_bytes=table.nbytes,
        columns=table.column_names,
        column_count=len(table.schema),
        null_counts=null_counts,
        mode=mode
    )


def _log_write_meta(name: str, schema: pa.Schema, row_count: int, mode: str):
    """Schema-only write log for streamed sources — no nbytes or null counts."""
    cols = ', '.join(f.name for f in schema)
    rows_str = f"{row_count:,}" if row_count >= 0 else "?"
    print(f"[{mode}] {name}: {rows_str} rows, {len(schema)} cols ({cols})")
    debug.log_data_output(
        dataset_name=name,
        row_count=row_count if row_count >= 0 else 0,
        size_bytes=0,
        columns=[f.name for f in schema],
        column_count=len(schema),
        null_counts={},
        mode=mode,
    )


def _source_hash(source, schema: pa.Schema, target_row_count: int) -> str:
    """Hash a merge source.

    For a Table: hash source rowcount + schema (stable across runs when the
    source is unchanged).
    For a RecordBatchReader: the stream has been consumed by deltalake by now,
    so we fall back to target rowcount + schema. This isn't a source-only
    fingerprint but it's still a stable descriptor of the written state.
    """
    import hashlib
    h = hashlib.md5()
    if isinstance(source, pa.Table):
        h.update(f"{len(source)}".encode())
    else:
        h.update(f"{target_row_count}".encode())
    h.update(str(schema).encode())
    return h.hexdigest()[:16]


def _validate_keys(table: pa.Table, keys: list[str], name: str):
    """Validate key columns before merge.

    Checks:
    1. Key columns exist
    2. Key columns have no nulls
    3. Key combination is unique (no duplicates)
    """
    # Check columns exist
    for k in keys:
        if k not in table.column_names:
            raise ValueError(f"[{name}] Key column '{k}' not found. Columns: {table.column_names}")

    # Check for nulls in key columns
    for k in keys:
        null_count = table[k].null_count
        if null_count > 0:
            raise ValueError(f"[{name}] Key column '{k}' has {null_count} nulls. Merge keys cannot be null.")

    # Check uniqueness - group by keys and look for duplicates
    if len(keys) == 1:
        # Single key - use value_counts
        key_col = table[keys[0]]
        unique_count = len(key_col.unique())
        if unique_count != len(table):
            dup_count = len(table) - unique_count
            raise ValueError(
                f"[{name}] Key '{keys[0]}' has {dup_count} duplicate values. "
                f"Merge key must be unique. Check your data or add more columns to key."
            )
    else:
        # Composite key - need to check combination
        import pyarrow.compute as pc

        # Concatenate key columns as strings for uniqueness check
        combined = pc.binary_join_element_wise(
            *[pc.cast(table[k], pa.string()) for k in keys],
            "||"
        )
        unique_count = len(combined.unique())
        if unique_count != len(table):
            dup_count = len(table) - unique_count
            raise ValueError(
                f"[{name}] Key {keys} has {dup_count} duplicate combinations. "
                f"Merge key must be unique. Check your data or add more columns to key."
            )


def merge(
    source: Union[pa.Table, pa.RecordBatchReader],
    name: str,
    *,
    key: Union[str, list[str]],
    partition_by: list[str] = None,
    validate: bool = True
) -> "WriteResult":
    """Upsert data into a Delta table.

    - Inserts new records (key doesn't exist)
    - Updates existing records (key exists)
    - Duplicates impossible

    Args:
        source: PyArrow Table or RecordBatchReader. Readers stream batches
            through deltalake without materializing the full dataset in
            memory — use this for large sources via DuckDB's
            fetch_record_batch() or similar. validate=True is incompatible
            with readers (would consume the stream).
        name: Dataset name
        key: Column(s) that uniquely identify a record
        partition_by: Optional columns to partition by
        validate: Check key uniqueness before merge (default True).
            Must be False for RecordBatchReader sources.

    Returns:
        WriteResult with uri, version, hash, rows.
    """
    is_reader = isinstance(source, pa.RecordBatchReader)
    if is_reader and validate:
        raise ValueError(
            "merge(): validate=True cannot be combined with a RecordBatchReader "
            "source — the stream would be consumed by validation. Pass "
            "validate=False or materialize to a pa.Table first."
        )

    if not is_reader and len(source) == 0:
        print(f"[merge] {name}: no data to write")
        return None

    # Normalize key to list
    keys = [key] if isinstance(key, str) else key

    # Validate keys (table-only)
    if validate:
        _validate_keys(source, keys, name)

    schema = source.schema
    column_names = [f.name for f in schema]

    uri = _get_uri(name)
    opts = _get_opts()

    # Probe for table existence. If it doesn't exist yet, create it via
    # write_deltalake. Any OTHER exception must propagate — we must NOT
    # silently fall back to overwrite mode because that would destroy an
    # existing table's contents on a transient merge failure.
    try:
        dt = DeltaTable(uri, storage_options=opts)
        table_exists = True
    except Exception as e:
        if not _is_table_not_found(e):
            raise
        table_exists = False

    if not table_exists:
        write_deltalake(
            uri,
            source,
            mode="overwrite",
            partition_by=partition_by,
            storage_options=opts,
            commit_properties=_run_commit_properties(),
        )
        dt = DeltaTable(uri, storage_options=opts)
        new_count = _target_row_count(dt)
        version = dt.version()
        h = _source_hash(source, schema, new_count)
        _log_write_meta(name, schema, new_count, "merge (created)")
    else:
        # Build merge predicate
        predicate = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        updates = {col: f"source.{col}" for col in column_names}

        dt.merge(
            source=source,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
            commit_properties=_run_commit_properties(),
        ).when_matched_update(
            updates=updates
        ).when_not_matched_insert(
            updates=updates
        ).execute()

        # Rowcount from Delta log (parquet footers), not by materializing target.
        # Hash on source rowcount+schema — stable fingerprint for unchanged inputs.
        new_count = _target_row_count(dt)
        version = dt.version()
        h = _source_hash(source, schema, new_count)
        _log_write_meta(name, schema, new_count, f"merge → {new_count:,} total")

    record_write(f"subsets/{name}", version=version, hash=h)
    return WriteResult(uri=uri, version=version, hash=h, rows=new_count)


def overwrite(
    source: Union[pa.Table, pa.RecordBatchReader],
    name: str,
    *,
    partition_by: list[str] = None
) -> "WriteResult":
    """Replace entire Delta table with new data.

    Use for:
    - Snapshots without natural key (current prices, live status)
    - Computed aggregates
    - Data that's fully recomputed each run (e.g. SDMX full-refresh flows
      where the connector pulls the complete dataset on every invocation)

    merge() is preferable only when runs bring PARTIAL data and you want
    target rows not in source to survive — otherwise overwrite is both
    simpler and cheaper (no target-side join).

    Accepts a pa.Table or pa.RecordBatchReader. Readers stream through
    deltalake without materializing the full source in memory — use via
    DuckDB's fetch_record_batch() or similar.
    """
    is_reader = isinstance(source, pa.RecordBatchReader)

    if not is_reader and len(source) == 0:
        print(f"[overwrite] {name}: no data to write")
        return None

    schema = source.schema

    uri = _get_uri(name)
    opts = _get_opts()

    write_deltalake(
        uri,
        source,
        mode="overwrite",
        partition_by=partition_by,
        storage_options=opts,
        schema_mode="overwrite",
        commit_properties=_run_commit_properties(),
    )

    dt = DeltaTable(uri, storage_options=opts)
    version = dt.version()
    new_count = _target_row_count(dt)
    h = _source_hash(source, schema, new_count)

    _log_write_meta(name, schema, new_count, "overwrite")
    record_write(f"subsets/{name}", version=version, hash=h)
    return WriteResult(uri=uri, version=version, hash=h, rows=new_count)


def append(
    source: Union[pa.Table, pa.RecordBatchReader],
    name: str,
    *,
    partition_by: list[str] = None
) -> "WriteResult":
    """Append data to a Delta table.

    WARNING: No deduplication! Use ONLY for:
    - Immutable events (audit logs, raw API responses)
    - Data where duplicates are impossible by design
    - Chunked loads into an already-partitioned table where each chunk's
      partition keys are disjoint from previously-written chunks

    Always use partition_by for cleanup capability.

    Accepts pa.Table or pa.RecordBatchReader. Readers stream through
    deltalake without materializing in memory — use for chunked loads
    (e.g. per-partition dedup) driven by DuckDB's fetch_record_batch().
    """
    is_reader = isinstance(source, pa.RecordBatchReader)

    if not is_reader and len(source) == 0:
        print(f"[append] {name}: no data to write")
        return None

    if partition_by is None:
        print(f"⚠️  Warning: append() without partition_by makes cleanup difficult")

    schema = source.schema

    uri = _get_uri(name)
    opts = _get_opts()

    write_deltalake(
        uri,
        source,
        mode="append",
        partition_by=partition_by,
        storage_options=opts,
        schema_mode="merge",  # Allow schema evolution for append
        commit_properties=_run_commit_properties(),
    )

    dt = DeltaTable(uri, storage_options=opts)
    version = dt.version()
    new_count = _target_row_count(dt)
    h = _source_hash(source, schema, new_count)

    _log_write_meta(name, schema, new_count, "append")
    record_write(f"subsets/{name}", version=version, hash=h)
    return WriteResult(uri=uri, version=version, hash=h, rows=new_count)
