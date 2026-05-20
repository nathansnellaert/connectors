import os
import csv
from datetime import datetime
from pathlib import Path

from .config import is_cloud

_log_dir = None
_run_timestamp = None


def _get_run_timestamp() -> str:
    global _run_timestamp
    if _run_timestamp is None:
        run_id = os.environ.get('RUN_ID', '')
        # Extract timestamp from run_id (format: connector-YYYYMMDD-HHMMSS)
        parts = run_id.rsplit('-', 2)
        if len(parts) >= 2 and len(parts[-2]) == 8 and len(parts[-1]) == 6:
            _run_timestamp = f"{parts[-2]}-{parts[-1]}"
        else:
            _run_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    return _run_timestamp


def _get_log_dir() -> Path:
    global _log_dir
    if _log_dir is None:
        # Check for explicit LOG_DIR (set by runner.py)
        if os.environ.get('LOG_DIR'):
            _log_dir = Path(os.environ['LOG_DIR'])
        elif is_cloud():
            _log_dir = Path("/tmp/logs") / _get_run_timestamp()
        else:
            _log_dir = Path("logs") / _get_run_timestamp()
        _log_dir.mkdir(parents=True, exist_ok=True)
    return _log_dir


def _is_logging_enabled():
    return os.environ.get('ENABLE_LOGGING', '').lower() == 'true'


def _append_csv(filename: str, row: dict, fieldnames: list):
    if not _is_logging_enabled():
        return
    filepath = _get_log_dir() / filename
    file_exists = filepath.exists()
    with open(filepath, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def log_http_request(method, url, status_code, duration_ms=None, error=None, **kwargs):
    _append_csv("http_requests.csv", {
        "timestamp": datetime.now().isoformat(),
        "run_id": os.environ.get('RUN_ID', 'unknown'),
        "method": method,
        "url": url,
        "status": status_code,
        "duration_ms": duration_ms,
        "error": error or ""
    }, ["timestamp", "run_id", "method", "url", "status", "duration_ms", "error"])


def log_data_output(dataset_name, row_count, size_bytes, columns=None, null_counts=None, **kwargs):
    """Log dataset output metadata.

    Args:
        dataset_name: Name of the dataset
        row_count: Number of rows
        size_bytes: Size in bytes
        columns: List of column names (only count is logged, not names)
        null_counts: Deprecated, ignored
    """
    _append_csv("data_outputs.csv", {
        "timestamp": datetime.now().isoformat(),
        "run_id": os.environ.get('RUN_ID', 'unknown'),
        "dataset": dataset_name,
        "rows": row_count,
        "size_bytes": size_bytes,
        "column_count": len(columns) if columns else 0,
    }, ["timestamp", "run_id", "dataset", "rows", "size_bytes", "column_count"])


def log_run_start():
    # Detect environment (cloud vs local)
    environment = "cloud" if is_cloud() else "local"

    # Detect trigger (GitHub sets GITHUB_EVENT_NAME for Actions)
    trigger = os.environ.get('GITHUB_EVENT_NAME', 'manual')

    _append_csv("runs.csv", {
        "timestamp": datetime.now().isoformat(),
        "run_id": os.environ.get('RUN_ID', 'unknown'),
        "event": "start",
        "status": "",
        "environment": environment,
        "trigger": trigger,
        "error": ""
    }, ["timestamp", "run_id", "event", "status", "environment", "trigger", "error"])


def log_run_end(status="completed", error=None):
    # Detect environment (cloud vs local)
    environment = "cloud" if is_cloud() else "local"

    # Detect trigger
    trigger = os.environ.get('GITHUB_EVENT_NAME', 'manual')

    _append_csv("runs.csv", {
        "timestamp": datetime.now().isoformat(),
        "run_id": os.environ.get('RUN_ID', 'unknown'),
        "event": "end",
        "status": status,
        "environment": environment,
        "trigger": trigger,
        "error": str(error) if error else ""
    }, ["timestamp", "run_id", "event", "status", "environment", "trigger", "error"])


def log_state_change(asset, old_state, new_state):
    if not _is_logging_enabled():
        return
    run_id = os.environ.get('RUN_ID', 'unknown')
    ts = datetime.now().isoformat()
    all_keys = set(old_state.keys()) | set(new_state.keys())
    for key in all_keys:
        old_val = old_state.get(key)
        new_val = new_state.get(key)
        if old_val != new_val:
            _append_csv("state_changes.csv", {
                "timestamp": ts,
                "run_id": run_id,
                "asset": asset,
                "key": key,
                "old_value": str(old_val) if old_val is not None else "",
                "new_value": str(new_val) if new_val is not None else ""
            }, ["timestamp", "run_id", "asset", "key", "old_value", "new_value"])


