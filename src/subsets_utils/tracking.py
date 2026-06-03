"""The in-memory run record for DAG execution.

One ambient record carries everything we know about a run's data plane:
- Lineage: which task read/wrote which asset, and the version/hash of each
  materialization, plus the function stack at the time of each I/O.
- State: every state-file key change (low volume, kept in full).

The lineage + state records ride the snapshot()/merge() pickle pipe back from
each per-node child to the supervisor, which serializes them into run.json.

HTTP is the exception: it does NOT live in memory and does NOT ride the pipe.
Every request is appended straight to `LOG_DIR/http_requests.csv` (one row,
tagged with the current nodespec). This is the same model memory.csv uses —
the full firehose streams to disk (crash-safe; survives a node OOM and bypasses
the pipe's size cap), and the per-node {count, error_count} index in run.json
is *derived* from this csv at finalize (see runner._stamp_run_enrichments). So
the csv is the single source of truth for HTTP; run.json only carries an index.

It lives here — rather than in StorageBackend — because it is purely
observability: io.py / delta.py / http_client.py *record* into it during a
node. StorageBackend stays purely bytes + credentials.

The module is import-cycle-free (stdlib only), which is why both the
orchestrator and the io/http layers can share it without a circular import.
"""

from contextvars import ContextVar
from dataclasses import dataclass, asdict
import csv as _csv
import io
import os
import threading
import traceback
from datetime import datetime, timezone
from pathlib import Path

# Current executing task (set by orchestrator). ContextVar so worker threads
# launched via contextvars.copy_context() inherit their own task ID cleanly.
_current_task_id: ContextVar[str | None] = ContextVar('current_task_id', default=None)

# Track which task wrote which assets: {asset_path: task_id}
_asset_writers: dict[str, str] = {}

# Track version info per asset: {asset_path: {"version": int, "hash": str}}
_asset_versions: dict[str, dict] = {}

# Detailed IO records with stack traces
@dataclass
class IORecord:
    asset_path: str
    task_id: str | None
    operation: str  # "read" or "write"
    stack: list[str]  # Simplified stack frames

_io_records: list[IORecord] = []

# State-file key changes per task (low volume → kept in full):
#   {task_id: [{"asset","key","old","new"}]}
_state_changes_by_task: dict[str | None, list[dict]] = {}

# Guards all of the above against concurrent access when DAG_PARALLELISM > 1.
_lock = threading.RLock()

# --- HTTP request log (direct-to-disk, NOT in memory, NOT under _lock) -------
# Every request is appended to LOG_DIR/http_requests.csv as a single row. The
# file handle is cached per-process: a forked/spawned child gets a pid mismatch
# on its first write and reopens its own append handle, so parent and children
# never share a file object. Open in O_APPEND ("a") + write one fully-formatted
# row + flush, so concurrent children (DAG_PARALLELISM > 1) don't interleave
# (atomic for rows under PIPE_BUF) and a crash loses nothing already flushed.
_HTTP_CSV_NAME = "http_requests.csv"
_HTTP_CSV_HEADER = ["timestamp", "nodespec", "method", "url", "status",
                    "duration_ms", "error"]
_http_fh_cache: dict = {"pid": None, "fh": None}


def _get_caller_stack(skip_frames: int = 3) -> list[str]:
    """Get simplified call stack, skipping internal frames.

    Returns list of "function_name (file:line)" strings.
    """
    frames = traceback.extract_stack()[:-skip_frames]
    result = []
    for frame in frames:
        # Skip internal subsets_utils frames
        if 'subsets_utils' in frame.filename and frame.name in ('record_read', 'record_write', '_get_caller_stack'):
            continue
        result.append(f"{frame.name} ({frame.filename.split('/')[-1]}:{frame.lineno})")
    return result[-5:]  # Keep last 5 relevant frames


def set_current_task(task_id: str | None):
    """Set the currently executing task ID. Called by orchestrator."""
    _current_task_id.set(task_id)


def init_http_log() -> None:
    """Create http_requests.csv with its header row, once, at run start.

    Called by the orchestrator before any node forks, so the single header
    write never races the per-node children (which only ever append data
    rows). Truncates any prior file — the csv is per-invocation, like
    output.log / memory.csv. No-op if LOG_DIR is unset (local dev without the
    runner), in which case record_http is a no-op too.
    """
    log_dir = os.environ.get("LOG_DIR")
    if not log_dir:
        return
    _http_fh_cache["fh"] = None  # drop any stale handle from a prior run
    with open(Path(log_dir) / _HTTP_CSV_NAME, "w", newline="") as f:
        _csv.writer(f, lineterminator="\n").writerow(_HTTP_CSV_HEADER)


def _http_fh():
    """Per-process append handle to http_requests.csv, or None if LOG_DIR unset."""
    log_dir = os.environ.get("LOG_DIR")
    if not log_dir:
        return None
    pid = os.getpid()
    if _http_fh_cache["pid"] != pid or _http_fh_cache["fh"] is None:
        _http_fh_cache["fh"] = open(Path(log_dir) / _HTTP_CSV_NAME, "a", newline="")
        _http_fh_cache["pid"] = pid
    return _http_fh_cache["fh"]


# =============================================================================
# Recording — called by io.py / delta.py / http_client.py during a node
# =============================================================================

def record_write(asset_path: str, *, version: int = None, hash: str = None):
    """Record that the current task wrote an asset. Called by io functions."""
    task_id = _current_task_id.get()
    stack = _get_caller_stack()
    with _lock:
        if task_id:
            _asset_writers[asset_path] = task_id

        if version is not None:
            _asset_versions[asset_path] = {"version": version, "hash": hash}

        _io_records.append(IORecord(
            asset_path=asset_path,
            task_id=task_id,
            operation="write",
            stack=stack
        ))


def record_read(asset_path: str):
    """Record that the current task read an asset. Called by io functions."""
    task_id = _current_task_id.get()
    stack = _get_caller_stack()
    with _lock:
        _io_records.append(IORecord(
            asset_path=asset_path,
            task_id=task_id,
            operation="read",
            stack=stack
        ))


def record_http(method: str, url: str, status: int | None, *,
                duration_ms: int = 0, error: str | None = None):
    """Append one HTTP request to http_requests.csv, tagged with the current
    nodespec. Called by http_client for every request.

    Direct-to-disk (crash-safe, bypasses the snapshot/merge pipe and its size
    cap): the full firehose is the source of truth, and run.json's per-node
    {count, error_count} index is derived from this csv at finalize. No-op if
    LOG_DIR is unset. A row is formatted in full and written in one flushed
    call so concurrent children don't interleave.
    """
    fh = _http_fh()
    if fh is None:
        return
    buf = io.StringIO()
    _csv.writer(buf, lineterminator="\n").writerow([
        datetime.now(timezone.utc).isoformat(),
        _current_task_id.get() or "",
        method,
        url,
        "" if status is None else status,
        duration_ms or 0,
        error or "",
    ])
    fh.write(buf.getvalue())
    fh.flush()


def record_state_change(asset: str, old_state: dict, new_state: dict):
    """Record per-key state changes for an asset against the current task.

    Called by io.save_state / io.record_completion. Values are stringified so
    the record stays trivially serializable.
    """
    task_id = _current_task_id.get()
    changes = []
    for key in set(old_state) | set(new_state):
        old_val = old_state.get(key)
        new_val = new_state.get(key)
        if old_val != new_val:
            changes.append({
                "asset": asset,
                "key": key,
                "old": None if old_val is None else str(old_val),
                "new": None if new_val is None else str(new_val),
            })
    if not changes:
        return
    with _lock:
        _state_changes_by_task.setdefault(task_id, []).extend(changes)


# =============================================================================
# Accessors — read by the orchestrator when building run.json
# =============================================================================

def get_asset_version(asset_path: str) -> dict | None:
    """Get version info for an asset: {"version": int, "hash": str}."""
    with _lock:
        return _asset_versions.get(asset_path)


def get_assets_by_writer(task_id: str) -> list[str]:
    """Get all assets written by a specific task."""
    with _lock:
        return [asset for asset, writer in _asset_writers.items() if writer == task_id]


def get_reads_by_task(task_id: str) -> list[str]:
    """Get all assets read by a specific task."""
    with _lock:
        return [r.asset_path for r in _io_records if r.task_id == task_id and r.operation == "read"]


def get_state_changes(task_id: str) -> list[dict]:
    """All state-file key changes recorded by a specific task."""
    with _lock:
        return [dict(c) for c in _state_changes_by_task.get(task_id, [])]


# =============================================================================
# Snapshot / merge — carry the record across the subprocess boundary
# =============================================================================

def snapshot() -> dict:
    """Return the full in-memory run record (under the lock).

    The child runs one node, then pipes this back; the supervisor applies it
    with merge(). Bounded by construction (lineage + low-volume state only;
    HTTP streams to its own csv and never rides this pipe) so the pickled
    payload stays well under the orchestrator's size cap.
    """
    with _lock:
        return {
            "asset_writers": dict(_asset_writers),
            "asset_versions": dict(_asset_versions),
            "io_records": [asdict(r) for r in _io_records],
            "state_changes_by_task": {
                tid: [dict(c) for c in changes]
                for tid, changes in _state_changes_by_task.items()
            },
        }


def merge(snap: dict) -> None:
    """Apply a child's snapshot into this process's record (under the lock)."""
    with _lock:
        _asset_writers.update(snap.get("asset_writers", {}))
        _asset_versions.update(snap.get("asset_versions", {}))
        for r in snap.get("io_records", []):
            _io_records.append(IORecord(**r))
        for tid, changes in snap.get("state_changes_by_task", {}).items():
            _state_changes_by_task.setdefault(tid, []).extend(changes)


def clear_tracking():
    """Clear all in-memory tracking data. Called at start of DAG run and on
    child entry. The HTTP csv is not in memory and is not touched here — it is
    (re)initialized once per run by init_http_log()."""
    with _lock:
        _asset_writers.clear()
        _asset_versions.clear()
        _io_records.clear()
        _state_changes_by_task.clear()
