"""Data I/O for raw assets, state files, and Delta tables.

Single code path for both local and cloud storage. All raw + state I/O
is routed through fsspec via `get_fs(uri)` — local paths use the local
filesystem, `s3://` URIs use s3fs. The primitives below are a thin
shell over fsspec; callers see a uniform byte-level API.

In cloud, `raw_uri()` / `state_uri()` return `s3://` URIs directly, so
io.py streams reads/writes straight to R2 via s3fs multipart upload —
there is no hydrate/flush bookend. In local mode they return local paths.

Streaming: for datasets that don't fit in memory use `raw_writer()`
(generic byte stream) or `raw_parquet_writer()` (row-group streaming
ParquetWriter). Both are context managers that yield a file-like
object or a writer, bounded by fsspec's block size.
"""

import io
import json
import gzip
import hashlib
import os
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable

from . import tracking
from .config import get_fs, raw_uri, state_uri, subsets_uri
from .storage import backend


# =============================================================================
# URI dispatch via fsspec
# =============================================================================

def _write_bytes(uri: str, data: bytes) -> None:
    """Write bytes to a URI (s3:// or local path). Delegates to StorageBackend."""
    backend.write_bytes(uri, data)


def _read_bytes(uri: str) -> Optional[bytes]:
    """Read bytes from a URI. Returns None if not found. Delegates to StorageBackend."""
    return backend.read_bytes(uri)


def _exists(uri: str) -> bool:
    """Check if a URI exists. Delegates to StorageBackend."""
    return backend.exists(uri)


def _delete(uri: str) -> None:
    """Delete a URI. No-op if already absent. Delegates to StorageBackend."""
    backend.delete(uri)


# =============================================================================
# Hashing
# =============================================================================

def data_hash(table: pa.Table) -> str:
    """Fast hash based on row count + schema. Use with state to detect changes."""
    h = hashlib.md5()
    h.update(f"{len(table)}".encode())
    h.update(str(table.schema).encode())
    return h.hexdigest()[:16]


# =============================================================================
# Subsets (published Delta tables)
# =============================================================================

def load_asset(asset_name: str) -> pa.Table:
    """Load a published Delta table by name."""
    from .tracking import record_read
    uri = subsets_uri(asset_name)
    opts = backend.deltalake_options(uri)
    try:
        table = DeltaTable(uri, storage_options=opts).to_pyarrow_table()
    except Exception as e:
        raise FileNotFoundError(f"No Delta table found at {uri}") from e
    record_read(f"subsets/{asset_name}")
    return table


# =============================================================================
# State files (small JSON, per-asset)
# =============================================================================

def _load_state_raw(asset: str) -> dict:
    """Load state including underscore-prefixed reserved keys (e.g. _metadata).
    Internal: used by record_completion and save_state's merge path."""
    uri = state_uri(asset)
    data = _read_bytes(uri)
    if not data:
        return {}
    return json.loads(data.decode("utf-8"))


def load_state(asset: str) -> dict:
    """Load state for an asset. Returns empty dict if not found.

    Reserved underscore-prefixed keys (e.g. `_metadata`) are stripped from
    the returned dict — they are managed by the orchestrator, not by fetch
    fns. This means the common load-modify-save pattern stays safe:

        state = load_state(asset)
        state["watermark"] = new_watermark
        save_state(asset, state)

    won't trip the reserved-prefix guard on save.
    """
    raw = _load_state_raw(asset)
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def save_state(asset: str, state_data: dict) -> str:
    """Save state for an asset. Returns the URI.

    Fetch-fn-managed state only. Underscore-prefixed keys are reserved for
    the orchestrator (see `record_completion`) — passing one here raises.
    Existing `_metadata` on disk (e.g. `code_hash` written post-success)
    is preserved across this write; only `updated_at` and `run_id` get
    refreshed each save.
    """
    import os
    for k in state_data:
        if isinstance(k, str) and k.startswith("_"):
            raise ValueError(
                f"reserved key in save_state user dict: {k!r}. "
                "Underscore-prefixed keys are managed by the orchestrator."
            )

    existing = _load_state_raw(asset)
    existing_meta = existing.get("_metadata", {}) if isinstance(existing.get("_metadata"), dict) else {}

    merged_meta = {
        **existing_meta,
        "updated_at": datetime.now().isoformat(),
        "run_id": os.environ.get("RUN_ID", "unknown"),
    }
    payload = {
        **state_data,
        "_metadata": merged_meta,
    }
    uri = state_uri(asset)
    _write_bytes(uri, json.dumps(payload, indent=2).encode("utf-8"))
    tracking.record_state_change(asset, existing, payload)
    return uri


def record_completion(asset: str, code_hash: str | None) -> str:
    """Stamp `_metadata.code_hash` for `asset` after a successful materialization.

    Called by the orchestrator post-success — not by fetch fns. Bypasses the
    `save_state` reserved-prefix guard because it operates ENTIRELY under the
    reserved namespace; fetch-fn-managed keys are preserved as-is.

    A `None` hash means "compute failed; we don't know what produced this state"
    and is recorded honestly rather than papering over it.
    """
    import os
    existing = _load_state_raw(asset)
    existing_meta = existing.get("_metadata", {}) if isinstance(existing.get("_metadata"), dict) else {}

    merged_meta = {
        **existing_meta,
        "code_hash": code_hash,
        "updated_at": datetime.now().isoformat(),
        "run_id": os.environ.get("RUN_ID", "unknown"),
    }
    user_keys = {k: v for k, v in existing.items() if not (isinstance(k, str) and k.startswith("_"))}
    payload = {
        **user_keys,
        "_metadata": merged_meta,
    }
    uri = state_uri(asset)
    _write_bytes(uri, json.dumps(payload, indent=2).encode("utf-8"))
    tracking.record_state_change(asset, existing, payload)
    return uri


# =============================================================================
# Raw files (text/binary blobs — CSV, XML, ZIP, etc.)
# =============================================================================

def save_raw_file(content: str | bytes, asset_id: str, extension: str = "txt", *, entity_id: str | None = None) -> str:
    """Save a raw file. Accepts str or bytes.

    entity_id (optional): namespaces the path under data/raw/<entity_id>/.
    Used by meta-authored connectors (entity-prefix layout). When None
    (default), legacy flat layout."""
    from .tracking import record_write
    data = content.encode("utf-8") if isinstance(content, str) else content
    uri = raw_uri(asset_id, extension, entity_id=entity_id)
    _write_bytes(uri, data)
    print(f"  -> Saved {asset_id}.{extension}")
    record_write(f"raw/{(entity_id + '/') if entity_id else ''}{asset_id}.{extension}")
    return uri


def load_raw_file(asset_id: str, extension: str = "txt", *, binary: bool = False, entity_id: str | None = None) -> str | bytes:
    """Load a raw file.

    Args:
        asset_id: Asset name.
        extension: File extension.
        binary: If True, always return bytes. If False (default), attempt
            UTF-8 decode and return str on success, bytes on failure.
            Set binary=True for xlsx/zip/parquet or any file where you
            need deterministic bytes — the decode fallback is unreliable
            when a binary payload happens to be ASCII-only.
        entity_id: Optional. If set, looks under data/raw/<entity_id>/ —
            meta entity-prefix layout. Default None = legacy flat path.
    """
    from .tracking import record_read
    uri = raw_uri(asset_id, extension, entity_id=entity_id)
    data = _read_bytes(uri)
    if data is None:
        raise FileNotFoundError(f"Raw asset '{asset_id}.{extension}' not found at {uri}")
    record_read(f"raw/{(entity_id + '/') if entity_id else ''}{asset_id}.{extension}")
    if binary:
        return data
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data


# =============================================================================
# Raw JSON (with optional gzip compression)
# =============================================================================

def save_raw_json(data, asset_id: str, compress: bool = False, *, entity_id: str | None = None) -> str:
    """Save raw JSON data, optionally gzip-compressed.

    entity_id: namespace under data/raw/<entity_id>/ (meta entity-prefix).
    Default None = legacy flat layout."""
    from .tracking import record_write
    if compress:
        ext = "json.gz"
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(json.dumps(data).encode("utf-8"))
        content = buf.getvalue()
    else:
        ext = "json"
        content = json.dumps(data, indent=2).encode("utf-8")
    uri = raw_uri(asset_id, ext, entity_id=entity_id)
    _write_bytes(uri, content)
    print(f"  -> Saved {asset_id}.{ext}")
    record_write(f"raw/{(entity_id + '/') if entity_id else ''}{asset_id}.{ext}")
    return uri


def load_raw_json(asset_id: str, *, entity_id: str | None = None):
    """Load raw JSON. Auto-detects compression (tries .json then .json.gz).

    entity_id: read from data/raw/<entity_id>/ (meta entity-prefix layout)."""
    from .tracking import record_read
    for ext in ("json", "json.gz"):
        uri = raw_uri(asset_id, ext, entity_id=entity_id)
        data = _read_bytes(uri)
        if data is None:
            continue
        record_read(f"raw/{(entity_id + '/') if entity_id else ''}{asset_id}.{ext}")
        if ext == "json.gz":
            with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as gz:
                return json.load(gz)
        return json.loads(data.decode("utf-8"))
    raise FileNotFoundError(f"Raw JSON asset '{asset_id}' not found.")


# =============================================================================
# Raw NDJSON (newline-delimited JSON, zstd-compressed by default)
#
# Use when records are heterogeneous, nested, or types drift across batches —
# anything where declaring a parquet schema would be brittle. For tabular data
# with stable column types, prefer save_raw_parquet.
# =============================================================================

_NDJSON_EXT = {"zstd": "ndjson.zst", "gzip": "ndjson.gz", None: "ndjson"}


def save_raw_ndjson(rows, asset_id: str, *, compression: str | None = "zstd") -> str:
    """Save records as NDJSON (one JSON object per line). Default: zstd.

    For datasets that fit in memory. For larger ones use `raw_writer(asset,
    "ndjson.gz", mode="wt", compression="gzip")` and stream line-by-line, or
    the zstd equivalent if zstandard is available in the runtime.

    Args:
        rows: Iterable of JSON-serializable dicts.
        asset_id: Logical asset name.
        compression: "zstd" (default), "gzip", or None.
    """
    from .tracking import record_write
    if compression not in _NDJSON_EXT:
        raise ValueError(f"compression must be 'zstd', 'gzip', or None; got {compression!r}")
    ext = _NDJSON_EXT[compression]

    n = 0
    if compression is None:
        out = io.BytesIO()
        for row in rows:
            out.write(json.dumps(row, separators=(",", ":")).encode("utf-8"))
            out.write(b"\n")
            n += 1
        content = out.getvalue()
    else:
        comp_buf = pa.BufferOutputStream()
        with pa.CompressedOutputStream(comp_buf, compression=compression) as out:
            for row in rows:
                out.write(json.dumps(row, separators=(",", ":")).encode("utf-8"))
                out.write(b"\n")
                n += 1
        content = comp_buf.getvalue().to_pybytes()

    uri = raw_uri(asset_id, ext)
    _write_bytes(uri, content)
    print(f"  -> Saved {asset_id}.{ext} ({n:,} records)")
    record_write(f"raw/{asset_id}.{ext}")
    return uri


def load_raw_ndjson(asset_id: str) -> list[dict]:
    """Load NDJSON records. Auto-detects compression by trying .zst, .gz, then plain."""
    from .tracking import record_read
    for compression, ext in (("zstd", "ndjson.zst"), ("gzip", "ndjson.gz"), (None, "ndjson")):
        uri = raw_uri(asset_id, ext)
        data = _read_bytes(uri)
        if data is None:
            continue
        record_read(f"raw/{asset_id}.{ext}")
        if compression is None:
            text = data.decode("utf-8")
        else:
            stream = pa.CompressedInputStream(pa.BufferReader(pa.py_buffer(data)), compression=compression)
            text = stream.read().decode("utf-8")
        return [json.loads(line) for line in text.splitlines() if line]
    raise FileNotFoundError(f"Raw NDJSON asset '{asset_id}' not found.")


def delete_raw_file(asset_id: str, extension: str = "parquet", *, entity_id: str | None = None) -> None:
    """Delete a raw asset by (asset_id, extension). No-op if absent.

    Symmetric with `save_raw_*` — addresses by id, works in cloud + dev.
    """
    _delete(raw_uri(asset_id, extension, entity_id=entity_id))


# =============================================================================
# Raw Parquet (PyArrow tables)
# =============================================================================

def save_raw_parquet(data: pa.Table, asset_id: str) -> str:
    """Save a PyArrow table as Parquet."""
    from .tracking import record_write
    if hasattr(data, "read_all"):
        data = data.read_all()
    buf = io.BytesIO()
    pq.write_table(data, buf, compression="zstd")
    uri = raw_uri(asset_id, "parquet")
    _write_bytes(uri, buf.getvalue())
    print(f"  -> Saved {asset_id}.parquet ({data.num_rows:,} rows)")
    record_write(f"raw/{asset_id}.parquet")
    return uri


def load_raw_parquet(asset_id: str) -> pa.Table:
    """Load a Parquet file as PyArrow table."""
    from .tracking import record_read
    uri = raw_uri(asset_id, "parquet")
    data = _read_bytes(uri)
    if data is None:
        raise FileNotFoundError(f"Raw parquet '{asset_id}' not found at {uri}")
    record_read(f"raw/{asset_id}.parquet")
    return pq.read_table(io.BytesIO(data))


@contextmanager
def raw_parquet_localpath(asset_id: str):
    """Context manager yielding a local filesystem path to a raw parquet.

    In dev mode: yields the dev path directly — no copy.
    In cloud mode: streams the remote parquet to a tempfile and yields
    that path; the file is deleted on exit.

    Use this when you need a file path for tools like DuckDB that read
    parquet by path rather than loading bytes into memory. The compressed
    parquet on disk is typically 5-10× smaller than its decompressed
    Arrow representation, so streaming queries against the path stay
    memory-bounded even when load_raw_parquet() would OOM.
    """
    from .tracking import record_read
    import tempfile
    import os as _os

    uri = raw_uri(asset_id, "parquet")
    record_read(f"raw/{asset_id}.parquet")

    if not uri.startswith("s3://"):
        local = Path(uri)
        if local.exists():
            yield str(local)
            return
        raise FileNotFoundError(f"Raw parquet '{asset_id}' not found at {uri}")

    fs = get_fs(uri)
    tmp = tempfile.NamedTemporaryFile(
        suffix=f".{asset_id}.parquet", delete=False
    )
    tmp.close()
    try:
        with fs.open(uri, "rb") as src, open(tmp.name, "wb") as dst:
            while True:
                chunk = src.read(16 * 1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
        yield tmp.name
    finally:
        try:
            _os.unlink(tmp.name)
        except OSError:
            pass


# =============================================================================
# Streaming helpers — for datasets too big to fit in memory
#
# These open an fsspec file handle via `get_fs(uri)`. Local paths get auto
# parent-dir creation; s3:// URIs stream via multipart upload.
# =============================================================================

@contextmanager
def raw_writer(
    asset_id: str,
    extension: str = "txt",
    *,
    mode: str = "wb",
    compression: str | None = None,
    encoding: str | None = "utf-8",
):
    """Streaming writer for a raw asset. Context manager yielding a file handle.

    Use when the content doesn't fit in memory. Writes go through fsspec,
    so s3:// URIs stream via multipart upload and local paths get parent
    dirs auto-created. Supports native gzip/bz2/xz compression.

    Args:
        asset_id: Logical asset name (same as save_raw_*).
        extension: File extension (e.g. "ndjson.gz", "csv").
        mode: File mode — "wb" for bytes (default), "wt" for text.
        compression: "gzip", "bz2", "xz", or None. Matches fsspec.
        encoding: Text encoding when mode="wt". Ignored for binary.

    Example:
        with raw_writer("big_dump", "ndjson.gz", mode="wt", compression="gzip") as f:
            for row in stream:
                f.write(json.dumps(row) + "\\n")
    """
    from .tracking import record_write
    uri = raw_uri(asset_id, extension)
    fs = get_fs(uri)
    open_kwargs = {}
    if "t" in mode:
        open_kwargs["encoding"] = encoding
    if compression is not None:
        open_kwargs["compression"] = compression
    with fs.open(uri, mode=mode, **open_kwargs) as f:
        yield f
    print(f"  -> Saved {asset_id}.{extension}")
    record_write(f"raw/{asset_id}.{extension}")


@contextmanager
def raw_reader(
    asset_id: str,
    extension: str = "txt",
    *,
    mode: str = "rb",
    compression: str | None = None,
    encoding: str | None = "utf-8",
):
    """Streaming reader for a raw asset. Symmetric with raw_writer()."""
    from .tracking import record_read
    uri = raw_uri(asset_id, extension)

    fs = get_fs(uri)
    open_kwargs = {}
    if "t" in mode:
        open_kwargs["encoding"] = encoding
    if compression is not None:
        open_kwargs["compression"] = compression
    with fs.open(uri, mode=mode, **open_kwargs) as f:
        yield f
    record_read(f"raw/{asset_id}.{extension}")


@contextmanager
def raw_parquet_writer(asset_id: str, schema: pa.Schema, *, compression: str = "zstd"):
    """Streaming Parquet writer yielding a `pq.ParquetWriter`.

    Bounded memory for arbitrarily large datasets — call `write_table()`
    or `write_batch()` inside the `with` block, and the writer flushes
    row groups as they grow. Works for both local and s3:// URIs.

    Example:
        with raw_parquet_writer("wiki_dump", schema) as w:
            for batch in stream_wikipedia():
                w.write_batch(batch)
    """
    from .tracking import record_write
    uri = raw_uri(asset_id, "parquet")
    fs = get_fs(uri)
    with fs.open(uri, "wb") as f:
        writer = pq.ParquetWriter(f, schema, compression=compression)
        try:
            yield writer
        finally:
            writer.close()
    print(f"  -> Saved {asset_id}.parquet (streamed)")
    record_write(f"raw/{asset_id}.parquet")


# =============================================================================
# Listing & existence checks
# =============================================================================

def list_raw_files(pattern: str) -> list[str]:
    """List raw files matching a glob pattern (relative to the raw dir).

    Args:
        pattern: Glob like "items/*.json.gz" — relative to <connector>/data/raw/

    Returns:
        Sorted list of relative paths.
    """
    # Probe raw_uri to get the connector's raw dir (s3:// or local). The
    # "__probe__" asset is never created — raw_uri only builds the path.
    probe = raw_uri("__probe__", "__")
    base_uri = probe.rsplit("/", 1)[0]

    if base_uri.startswith("s3://"):
        fs = get_fs(base_uri)
        try:
            matches = fs.glob(f"{base_uri}/{pattern}")
        except FileNotFoundError:
            return []
        # s3fs returns keys without "s3://bucket/" prefix
        from urllib.parse import urlparse
        s3_prefix = urlparse(base_uri).path.lstrip("/") + "/"
        bucket = urlparse(base_uri).netloc
        return sorted([
            m[len(f"{bucket}/{s3_prefix}"):] if m.startswith(f"{bucket}/{s3_prefix}") else m[len(s3_prefix):]
            for m in matches
        ])

    raw_dir = Path(base_uri)
    if not raw_dir.exists():
        return []
    return sorted([str(p.relative_to(raw_dir)) for p in raw_dir.glob(pattern)])


def raw_asset_exists(asset_id: str, ext: str = "parquet", max_age_days: int | None = None, *, entity_id: str | None = None) -> bool:
    """Check if a raw asset exists. Optionally check it is fresh enough.

    Works for both s3:// URIs (via fsspec `fs.info`) and local paths.

    Args:
        max_age_days: If set, returns False if the asset is older than this many days.
        entity_id: Optional. If set, looks under data/raw/<entity_id>/ —
            meta entity-prefix layout. Default None = legacy flat path.
    """
    uri = raw_uri(asset_id, ext, entity_id=entity_id)

    if uri.startswith("s3://"):
        fs = get_fs(uri)
        if not fs.exists(uri):
            return False
        if max_age_days is None:
            return True
        try:
            info = fs.info(uri)
        except FileNotFoundError:
            return False
        mtime = info.get("LastModified") or info.get("mtime")
        if mtime is None:
            return True  # existence confirmed, age unknown — assume fresh
        if hasattr(mtime, "tzinfo") and mtime.tzinfo is not None:
            now = datetime.now(mtime.tzinfo)
        else:
            now = datetime.now()
        return (now - mtime) < timedelta(days=max_age_days)

    # Local dev: check local dev dir.
    p = Path(uri)
    if not p.exists():
        return False
    if max_age_days is not None:
        age = datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)
        return age < timedelta(days=max_age_days)
    return True
