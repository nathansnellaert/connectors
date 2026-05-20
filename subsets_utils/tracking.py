"""Lightweight tracking for DAG execution.

This module provides a shared context between orchestrator and io modules
without creating circular imports. The orchestrator sets the current task,
and io functions record what files they read/write.

Tracks:
- Which task read/wrote which assets
- The function stack at time of IO (to trace back through helper functions)
"""

from contextvars import ContextVar
from dataclasses import dataclass, field
import threading
import traceback

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

# Guards _asset_writers / _asset_versions / _io_records against concurrent
# access when DAG_PARALLELISM > 1.
_lock = threading.RLock()


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


def get_current_task() -> str | None:
    """Get the currently executing task ID."""
    return _current_task_id.get()


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


def get_asset_version(asset_path: str) -> dict | None:
    """Get version info for an asset: {"version": int, "hash": str}."""
    with _lock:
        return _asset_versions.get(asset_path)


def get_writer(asset_path: str) -> str | None:
    """Get the task ID that wrote an asset."""
    with _lock:
        return _asset_writers.get(asset_path)


def get_assets_by_writer(task_id: str) -> list[str]:
    """Get all assets written by a specific task."""
    with _lock:
        return [asset for asset, writer in _asset_writers.items() if writer == task_id]


def get_reads_by_task(task_id: str) -> list[str]:
    """Get all assets read by a specific task."""
    with _lock:
        return [r.asset_path for r in _io_records if r.task_id == task_id and r.operation == "read"]


def get_writes_by_task(task_id: str) -> list[str]:
    """Get all assets written by a specific task (from detailed records)."""
    with _lock:
        return [r.asset_path for r in _io_records if r.task_id == task_id and r.operation == "write"]


def get_io_records(task_id: str | None = None) -> list[dict]:
    """Get IO records, optionally filtered by task.

    Returns list of dicts with asset_path, task_id, operation, stack.
    """
    with _lock:
        records = list(_io_records) if task_id is None else [
            r for r in _io_records if r.task_id == task_id
        ]
    return [
        {
            "asset": r.asset_path,
            "task": r.task_id,
            "op": r.operation,
            "stack": r.stack
        }
        for r in records
    ]


def clear_tracking():
    """Clear all tracking data. Called at start of DAG run."""
    with _lock:
        _asset_writers.clear()
        _asset_versions.clear()
        _io_records.clear()
