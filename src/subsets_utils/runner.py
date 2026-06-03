#!/usr/bin/env python3
"""Supervisor for connector execution.

Owns the run lifecycle around the connector subprocess:
- Sets up RUN_ID + LOG_DIR (fresh or resume)
- In cloud + resume: downloads prior run.json from R2 to LOG_DIR so the
  orchestrator picks it up and inherits done node states
- Spawns the connector via `python -m src.main`
- Captures stdout to logs/<run_id>/output.log
- Runs an external memory profiler thread
- Handles SIGTERM gracefully
- After subprocess exit: reads run.json to determine the right exit code
- In cloud: uploads logs/<run_id>/* to s3://bucket/<connector>/runs/<run_id>/

Exit code semantics (read by GH Actions workflow):
- 0  = run.json status="done" → fully complete
- 2  = run.json status="needs_continuation" or subprocess SIGTERM/OOM → retrigger
- 1  = subprocess error or run.json status="failed" → failure, do not retrigger

Usage:
    python -m subsets_utils.runner                          # fresh run
    RUN_ID=20260414-101918 python -m subsets_utils.runner   # adopt / resume a specific id
"""

import csv
import json
import os
import signal
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .config import (
    is_cloud, get_connector_name, get_data_dir, get_bucket_name,
    get_r2_prefix, get_r2_run_base,
)
from .storage import backend
from . import platform_github


# =============================================================================
# R2 I/O — thin bucket-key wrappers over the shared StorageBackend
# =============================================================================

def _r2_uri(key: str) -> str:
    """Build an s3:// URI for a bucket-relative key."""
    return f"s3://{get_bucket_name()}/{key}"


def _r2_upload_file(path: str, key: str) -> None:
    backend.upload_file(path, _r2_uri(key))


def _r2_download_bytes(key: str) -> bytes | None:
    return backend.read_bytes(_r2_uri(key))


# =============================================================================
# Memory profiler (external — observes subprocess from parent)
# =============================================================================

class MemoryProfiler:
    """Sample subprocess memory every N seconds, write to memory.csv."""

    def __init__(self, pid: int, log_dir: Path, interval: float = 10.0):
        self.pid = pid
        self.log_file = log_dir / "memory.csv"
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _sample_loop(self):
        try:
            import psutil
        except ImportError:
            print("Warning: psutil not available, memory profiling disabled")
            return

        try:
            process = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            return

        with open(self.log_file, "w", newline="") as f:
            csv.writer(f).writerow(["timestamp", "rss_mb", "vms_mb", "pct"])

        while not self._stop.is_set():
            try:
                rss = process.memory_info().rss
                vms = process.memory_info().vms
                pct = process.memory_percent()
                for child in process.children(recursive=True):
                    try:
                        rss += child.memory_info().rss
                        vms += child.memory_info().vms
                        pct += child.memory_percent()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                with open(self.log_file, "a", newline="") as f:
                    csv.writer(f).writerow([
                        datetime.now().isoformat(),
                        round(rss / 1024 / 1024, 1),
                        round(vms / 1024 / 1024, 1),
                        round(pct, 1),
                    ])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break

            self._stop.wait(self.interval)


# =============================================================================
# Helpers
# =============================================================================

def write_error_log(log_dir: Path, exit_code: int, output_file: Path, tail_lines: int = 100):
    """Write the last N lines of stdout to error.txt for quick triage."""
    error_file = log_dir / "error.txt"
    if not output_file.exists():
        error_file.write_text(f"Exit code: {exit_code}\nNo output captured.\n")
        return
    lines = output_file.read_text().splitlines(keepends=True)
    tail = lines[-tail_lines:] if len(lines) > tail_lines else lines
    with open(error_file, "w") as f:
        f.write(f"Exit code: {exit_code}\n")
        f.write(f"Last {len(tail)} lines of output:\n")
        f.write("-" * 60 + "\n")
        f.writelines(tail)


def _generate_run_id() -> str:
    """Generate a fresh run ID (UTC timestamp)."""
    return datetime.now(ZoneInfo("UTC")).strftime("%Y%m%d-%H%M%S")


def _connector_runs_prefix(connector: str, run_id: str) -> str:
    """R2 prefix for a run's artifacts: [<R2_PREFIX>/]<connector>/runs/<run_id>."""
    prefix = get_r2_prefix()
    base = f"{connector}/runs/{run_id}"
    return f"{prefix}/{base}" if prefix else base


def _hydrate_resume_state(connector: str, run_id: str, log_dir: Path) -> bool:
    """In cloud mode, download prior run.json from R2 into LOG_DIR for resume.

    Returns True if a prior run.json was found and downloaded.
    """
    if not is_cloud():
        return (log_dir / "run.json").exists()

    key = f"{_connector_runs_prefix(connector, run_id)}/run.json"
    data = _r2_download_bytes(key)
    if data is None:
        return False

    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "run.json").write_bytes(data)
    print(f"[runner] Hydrated prior run.json from {key}")
    return True


def _read_run_status(log_dir: Path) -> str | None:
    """Read run.json status, or None if missing/invalid."""
    p = log_dir / "run.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text()).get("status")
    except Exception:
        return None


def _append_invocation(log_dir: Path, invocation: dict) -> None:
    """Append an invocation entry to run.json's invocations array."""
    p = log_dir / "run.json"
    if not p.exists():
        return  # nothing to update if orchestrator never wrote run.json
    try:
        data = json.loads(p.read_text())
    except Exception:
        return
    data.setdefault("invocations", []).append(invocation)
    p.write_text(json.dumps(data, indent=2))


def _resolve_exit_code(subprocess_exit: int, run_status: str | None) -> int:
    """Translate (subprocess exit, run.json status) into the runner's exit code.

    The contract:
        0 = done             (run.json status="done", subprocess exited cleanly)
        2 = continuation     (status="needs_continuation" OR subprocess died on
                              SIGTERM/OOM with the run not done — retrigger)
        1 = failure          (anything else — do not retrigger)
    """
    # Status takes precedence — it's the source of truth for "is the run done"
    if run_status == "done":
        return 0
    if run_status == "needs_continuation":
        return 2

    # No status or status="failed"/"running" — fall back to subprocess exit
    # Killed by SIGTERM (143) or OOM (137) → continuation candidate IF some
    # progress was made (run.json exists)
    if subprocess_exit in (137, 143) and run_status is not None:
        return 2

    # Anything else is a hard failure
    return 1


# =============================================================================
# Main
# =============================================================================

def _peak_memory_bytes(log_dir: Path) -> int | None:
    """Peak RSS across the run from memory.csv, in bytes. None if unavailable."""
    memory_csv = log_dir / "memory.csv"
    if not memory_csv.exists():
        return None
    try:
        peak_rss = 0.0
        with open(memory_csv) as f:
            for row in csv.DictReader(f):
                rss_mb = float(row["rss_mb"])
                if rss_mb > peak_rss:
                    peak_rss = rss_mb
        return int(peak_rss * 1024 * 1024)
    except Exception:
        return None


def _http_index(log_dir: Path) -> dict[str, dict]:
    """Derive a per-nodespec {count, error_count} index from http_requests.csv.

    The csv is the source of truth for HTTP (every request, one row). This is
    the single-pass reduction stamped into run.json — the same shape as
    _peak_memory_bytes derives from memory.csv. A request is a failure if it
    errored or returned status >= 400. Returns {} if the csv is missing/empty.
    """
    csv_path = log_dir / "http_requests.csv"
    if not csv_path.exists():
        return {}
    index: dict[str, dict] = {}
    try:
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                node = row.get("nodespec") or ""
                bucket = index.setdefault(node, {"count": 0, "error_count": 0})
                bucket["count"] += 1
                status = row.get("status") or ""
                is_failure = bool(row.get("error")) or (
                    status.isdigit() and int(status) >= 400
                )
                if is_failure:
                    bucket["error_count"] += 1
    except Exception:
        return {}
    return index


def _stamp_run_enrichments(log_dir: Path) -> None:
    """Stamp supervisor-side context into run.json in place, before evacuation.

    run.json is the single canonical run artifact — the orchestrator owns its
    body (status, DAG, lineage, HTTP, state), and the supervisor adds the few
    facts only it knows: GitHub Actions identity, git commit, peak memory. The
    enriched file evacuates with the rest of LOG_DIR; there is no second,
    reshaped copy. No-op if run.json is missing/invalid.
    """
    p = log_dir / "run.json"
    if not p.exists():
        return
    try:
        data = json.loads(p.read_text())
    except Exception:
        return

    git_commit = os.environ.get("GITHUB_SHA")
    if git_commit:
        data["git_commit"] = git_commit

    peak = _peak_memory_bytes(log_dir)
    if peak is not None:
        data["peak_memory_bytes"] = peak

    # Derive the per-node HTTP index from http_requests.csv and inject it into
    # each node's record. Done here (the supervisor, after the subprocess exits)
    # so the index survives even when the orchestrator was hard-killed mid-run —
    # the same robustness as peak_memory_bytes. The csv stays the full firehose.
    http_index = _http_index(log_dir)
    if http_index:
        for node in data.get("dag", {}).get("nodes", []):
            stats = http_index.get(node.get("id"))
            if stats:
                node["http"] = stats

    gh_run_id = os.environ.get("GITHUB_RUN_ID")
    if gh_run_id:
        data["github_run_id"] = gh_run_id
        gh_repo = os.environ.get("GITHUB_REPOSITORY", "")
        data["github_run_url"] = f"https://github.com/{gh_repo}/actions/runs/{gh_run_id}"

    try:
        p.write_text(json.dumps(data, indent=2))
    except OSError:
        pass


def _expand_bundled_secrets() -> None:
    """Expand the single-repo secret blob into the environment.

    The connectors repo runs every connector through one workflow, which
    passes all Actions secrets as one JSON object in `HARNESS_SECRETS`
    (`toJSON(secrets)`). Expand it before anything reads R2/API credentials.
    `setdefault` so an explicitly-set env var always wins.
    """
    blob = os.environ.pop("HARNESS_SECRETS", None)
    if not blob:
        return
    try:
        bundle = json.loads(blob)
    except json.JSONDecodeError:
        return
    if isinstance(bundle, dict):
        for key, val in bundle.items():
            if isinstance(val, str):
                os.environ.setdefault(key, val)


def main():
    _expand_bundled_secrets()

    connector = get_connector_name()
    os.environ["CONNECTOR_NAME"] = connector

    # RUN_ID: if the user passed one, this is a resume; otherwise generate fresh.
    incoming_run_id = os.environ.get("RUN_ID")
    is_resume = bool(incoming_run_id)
    run_id = incoming_run_id or _generate_run_id()
    os.environ["RUN_ID"] = run_id

    # Export the resolved RUN_ID to $GITHUB_OUTPUT so the workflow's retrigger
    # step can pass it back as run_id input — preserving DAG resume across
    # retriggers even when the first invocation was started without one.
    platform_github.write_resolved_run_id(run_id)

    # LOG_DIR: same path locally and in cloud, only the prefix differs
    log_dir = Path("/tmp/logs" if is_cloud() else "logs") / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    os.environ["LOG_DIR"] = str(log_dir)

    # Resume hydration: in cloud, pull prior run.json from R2 if it exists
    hydrated = False
    if is_resume:
        hydrated = _hydrate_resume_state(connector, run_id, log_dir)

    # Dev data dir still needs to exist for local scratch writes.
    # In cloud, raw/state go straight to R2 via fsspec — no hydrate needed.
    data_dir = Path(get_data_dir())
    if not is_cloud():
        data_dir.mkdir(parents=True, exist_ok=True)

    # Record invocation start
    invocation_id = "i-" + datetime.now(ZoneInfo("UTC")).strftime("%Y%m%d-%H%M%S")
    invocation_started_at = datetime.now(timezone.utc).isoformat()

    cmd = [sys.executable, "-m", "src.main"]

    print(f"Starting connector (RUN_ID: {run_id})")
    if hydrated:
        source = "R2" if is_cloud() else "local"
        print(f"  Resuming with prior run.json from {source}")
    print(f"Log directory: {log_dir}")
    if is_cloud():
        print(f"Raw scope:     s3://{get_bucket_name()}/{get_r2_run_base()}/raw")
    else:
        print(f"Raw scope:     {get_data_dir()}/raw")
    print("-" * 60)

    env = os.environ.copy()
    src_path = str(Path.cwd() / "src")
    env["PYTHONPATH"] = src_path + (":" + env["PYTHONPATH"] if "PYTHONPATH" in env else "")
    # Force unbuffered subprocess stdio so the user sees `load_nodes` progress,
    # the per-node DAG prints, and connector output in real time. Without this,
    # large connectors (e.g. cbs-netherlands with 1000+ nodes) appear to hang
    # for tens of seconds while load_nodes() imports module files silently.
    env["PYTHONUNBUFFERED"] = "1"

    # GHA hard-kills the job at 6h. Stop scheduling new nodes at 5h45m so
    # in-flight nodes drain, run.json finalizes as needs_continuation, logs
    # upload to R2, and the workflow's retrigger step fires before SIGKILL.
    if is_cloud() and "DAG_TIME_BUDGET" not in env:
        env["DAG_TIME_BUDGET"] = "20700"

    output_file = log_dir / "output.log"

    with open(output_file, "w") as log_f:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1,
        )

        profiler = MemoryProfiler(process.pid, log_dir)
        profiler.start()

        # SIGTERM handling depends on DAG_ON_FAILURE:
        # - "continue" (default for cloud workflows): GitHub Actions sometimes
        #   sends SIGTERM to the whole step when a single child OOMs and the
        #   host briefly thrashes. The orchestrator's per-node subprocess
        #   isolation already contained the damage — we should keep going. The
        #   orchestrator's own handler ignores SIGTERM in continue mode, and
        #   here we ALSO ignore it (logging only) so we don't kill the
        #   orchestrator out from under itself.
        # - "crash" (or unset): forward SIGTERM and enforce a 10s SIGKILL
        #   timeout — historic behavior for callers that want quick teardown.
        #
        # If GitHub Actions really needs us dead it will SIGKILL the whole
        # process group ~10s after SIGTERM, which we cannot catch.
        on_failure = os.environ.get("DAG_ON_FAILURE", "crash")
        ignore_sigterm = (on_failure == "continue")

        def handle_sigterm(signum, frame):
            if ignore_sigterm:
                print(f"\nReceived SIGTERM (ignored — DAG_ON_FAILURE=continue)")
                return
            print(f"\nReceived SIGTERM, terminating child...")
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

        signal.signal(signal.SIGTERM, handle_sigterm)

        try:
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log_f.write(line)
                log_f.flush()
        except KeyboardInterrupt:
            print("\nInterrupted, terminating child...")
            process.terminate()

        subprocess_exit = process.wait()

    profiler.stop()
    print("-" * 60)

    # Determine the runner's exit code from run.json status + subprocess exit
    run_status = _read_run_status(log_dir)
    exit_code = _resolve_exit_code(subprocess_exit, run_status)

    # Record invocation finish
    invocation = {
        "invocation_id": invocation_id,
        "started_at": invocation_started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "subprocess_exit_code": subprocess_exit,
        "runner_exit_code": exit_code,
        "run_status_after": run_status,
        "host": os.environ.get("RUNNER_NAME") or os.environ.get("HOSTNAME") or "local",
    }
    _append_invocation(log_dir, invocation)

    # Self-retrigger for cloud continuation. The runner dispatches its own
    # workflow with the preserved run_id, so any connector benefits from
    # continuation regardless of whether its workflow YAML has a retrigger
    # step. On success we exit 0 — the workflow stays green and the new run
    # is already in flight. On failure we fall through to exit 2; workflows
    # that still have an in-YAML retrigger step act as a fallback.
    continuation_dispatched = False
    if exit_code == 2 and is_cloud():
        if platform_github.maybe_retrigger(run_id):
            exit_code = 0
            continuation_dispatched = True

    # Print result
    if exit_code == 0:
        if continuation_dispatched:
            print(f"Continuation dispatched — workflow will retrigger with run_id={run_id}")
        else:
            print(f"Connector completed successfully (run.json status='done')")
    elif exit_code == 2:
        if run_status == "needs_continuation":
            print(f"Continuation needed (run.json status='needs_continuation') - exit 2 to retrigger")
        else:
            print(f"Subprocess died (exit {subprocess_exit}) but run partially complete - exit 2 to retrigger")
    else:
        if subprocess_exit == 137:
            error_msg = "Exit code 137 - Out of memory (no progress to resume)"
        elif subprocess_exit == 143:
            error_msg = "Exit code 143 - SIGTERM (no progress to resume)"
        else:
            error_msg = f"Subprocess exit code {subprocess_exit}, run.json status={run_status}"
        print(f"Connector failed: {error_msg}")
        write_error_log(log_dir, subprocess_exit, output_file)

    # Stamp supervisor-side context (git commit, GitHub identity, peak memory)
    # into run.json so the single canonical artifact carries it — then let the
    # whole directory evacuate. No reshape, no second copy.
    _stamp_run_enrichments(log_dir)

    # Cloud: raw + state already live in R2 (connectors write direct via
    # fsspec/s3fs). Only logs need to be evacuated. Uploading the whole LOG_DIR
    # is THE evacuation mechanism — every artifact (run.json, output.log,
    # memory.csv, error.txt) lands under <connector>/runs/<run_id>/ verbatim.
    if is_cloud():
        prefix = _connector_runs_prefix(connector, run_id)
        print(f"Uploading logs to R2 under {prefix}/...")
        for log in log_dir.rglob("*"):
            if log.is_file():
                try:
                    key = f"{prefix}/{log.relative_to(log_dir)}"
                    _r2_upload_file(str(log), key)
                    print(f"  -> {key}")
                except Exception as e:
                    print(f"  Failed to upload {log.name}: {e}")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
