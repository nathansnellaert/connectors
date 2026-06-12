"""DAG orchestration with run-state persistence.

The DAG class:
- Runs each NodeSpec in dependency order (topological sort), each in a fresh
  forked subprocess (memory isolation per node). A node with no deps is ready
  immediately; nodes with deps wait until every dependency has completed
  successfully. Ties (independent nodes) keep declaration order.
- Runs each node in a fresh forked subprocess (memory isolation per node)
- Writes run.json after each node
- Marks status as "needs_continuation" if any node returns True (pagination)
- Knows nothing about exit codes or time budgets — that's runner.py's job.

Subprocess-per-node:
- Each node is executed in a forked child process via multiprocessing.
- Child runs one fn(id), pipes back a result dict, exits. OS reclaims RSS.
- One node OOMing only kills that node; the rest of the DAG continues.
- Tracking state (asset_writers, io_records) is serialized by the child and
  merged into the supervisor's tracking module after each node completes.

Continuation pattern:
- A node returns True to signal "more work to do, please retrigger me later"
- The orchestrator records this in run.json status field
- Runner.py reads run.json after subprocess exit and translates the status
  to an exit code (0=done, 2=continuation, 1=failed)
- A SIGTERM that interrupts the supervisor does NOT trigger continuation —
  the run is marked failed, and a human must investigate. Auto-retrigger
  on host kill would loop forever on a real OOM root cause.

Each main.py invocation is a fresh DAG run. Per-asset idempotency is the
fetch fn's responsibility (raw_asset_exists + state). Cross-invocation
inheritance of node statuses was removed — it caused log/run.json to claim
nodes ran in this invocation when they hadn't, and the harness's on-disk
probe disagreed with the orchestrator about where to find raw.
"""

import hashlib
import importlib.util
import json
import multiprocessing
import os
import pickle
import signal
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from . import tracking
from .io import record_completion, _load_state_raw
from .spec import MaintainSpec, NodeSpec, SqlNodeSpec
from .spec_hash import compute_spec_hash, compute_sql_spec_hash
from .tracking import (
    clear_tracking,
    get_asset_version,
    get_assets_by_writer,
    get_reads_by_task,
    set_current_task,
)


# Spawn context for subprocess-per-node execution. Spawn re-imports the world
# in a fresh interpreter (~1s startup) but inherits no thread/lock state from
# the supervisor. Fork was tried and deadlocked on long catalog runs: by the
# time we forked for a later node, the parent had imported httpx/s3fs/asyncio
# and made HTTP+async calls, leaving background threads mid-lock. The forked
# child inherited the locked memory without the threads holding the locks and
# hung the first time it touched HTTP — supervisor then waited forever on the
# child's sentinel. Spawn pays the import cost per node to make that class of
# hang impossible.
_MP_CTX = multiprocessing.get_context("spawn")

# Cap on the pickled size of a child→supervisor result dict. Defends against a
# node accidentally stuffing a large pa.Table into a tracking record. 10 MB is
# generous: tracking records are tiny strings and stack snippets.
_MAX_RESULT_PICKLE_BYTES = 10 * 1024 * 1024


def _topology_hash(specs: list[NodeSpec]) -> str:
    """Hash of the graph — used to detect changes between invocations.
    Topology is the set of (id, kind, sorted deps), so adding/removing an edge
    changes the hash."""
    items = sorted(
        (spec.id, spec.kind, sorted(spec.deps)) for spec in specs
    )
    return hashlib.md5(json.dumps(items).encode()).hexdigest()[:16]


def _validate_deps(specs_by_id: dict[str, NodeSpec]) -> None:
    """Reject unknown dep ids, self-deps, and cycles. Raises ValueError naming
    the offending node (and, for cycles, the full cycle path)."""
    for s in specs_by_id.values():
        for d in s.deps:
            if d == s.id:
                raise ValueError(f"NodeSpec {s.id!r} depends on itself")
            if d not in specs_by_id:
                raise ValueError(f"NodeSpec {s.id!r} declares unknown dep {d!r}")

    # Iterative DFS with WHITE/GREY/BLACK coloring to find a cycle and report
    # the path. GREY = on the current DFS stack; hitting a GREY node is a cycle.
    WHITE, GREY, BLACK = 0, 1, 2
    color = {sid: WHITE for sid in specs_by_id}
    for root in specs_by_id:
        if color[root] != WHITE:
            continue
        stack: list[tuple[str, int]] = [(root, 0)]
        path: list[str] = []
        while stack:
            node, i = stack[-1]
            if i == 0:
                color[node] = GREY
                path.append(node)
            deps = specs_by_id[node].deps
            if i < len(deps):
                stack[-1] = (node, i + 1)
                nxt = deps[i]
                if color[nxt] == GREY:
                    cycle = path[path.index(nxt):] + [nxt]
                    raise ValueError(
                        "NodeSpec dependency cycle: " + " -> ".join(cycle)
                    )
                if color[nxt] == WHITE:
                    stack.append((nxt, 0))
            else:
                color[node] = BLACK
                path.pop()
                stack.pop()


def _atomic_write_json(path: Path, data: dict) -> None:
    """Write JSON atomically (write to tmp, rename) so partial writes can't corrupt."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.rename(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _load_run_state(log_dir: Path) -> dict | None:
    """Load run.json from a log directory, or None if missing/invalid."""
    p = log_dir / "run.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _child_entrypoint(fn: Callable, args: tuple, task_id: str, pipe_w) -> None:
    """Runs in a forked child process. Executes one DAG node and pipes back
    a result dict. The child inherits the supervisor's modules and tracking
    dicts via fork; we clear tracking on entry so the snapshot we send back
    contains only this node's I/O.

    The result dict shape:
        {
            "task_id": str,
            "status": "done" | "failed",
            "started_at": iso8601 str,
            "finished_at": iso8601 str,
            "duration_s": float,
            "needs_continuation": bool,        # only when status == "done"
            "error": str (only on failed),
            "traceback": str (only on failed),
            "tracking": tracking.snapshot()  # lineage + bounded HTTP + state
        }
    """
    # Reset signal handlers in the child — supervisor's SIGTERM handler is
    # CoW-inherited but does not apply to children. Default disposition for
    # SIGTERM is "terminate" which is what we want when supervisor escalates.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    clear_tracking()
    set_current_task(task_id)

    started_at = datetime.now(timezone.utc).isoformat()
    result: dict = {
        "task_id": task_id,
        "started_at": started_at,
        "status": "failed",
        "needs_continuation": False,
    }

    try:
        ret = fn(*args)
        result["status"] = "done"
        if ret is True:
            result["needs_continuation"] = True
    except BaseException as e:  # noqa: BLE001 — surface every failure mode
        result["status"] = "failed"
        result["error"] = str(e) or e.__class__.__name__
        result["traceback"] = traceback.format_exc()

    finished_at = datetime.now(timezone.utc).isoformat()
    result["finished_at"] = finished_at
    try:
        result["duration_s"] = (
            datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)
        ).total_seconds()
    except Exception:
        result["duration_s"] = 0.0

    result["tracking"] = tracking.snapshot()

    # Flush stdio before sending result. Fork-inherited pipes can drop the
    # last buffered line if the child exits without flushing.
    sys.stdout.flush()
    sys.stderr.flush()

    try:
        payload = pickle.dumps(result)
        if len(payload) > _MAX_RESULT_PICKLE_BYTES:
            raise ValueError(
                f"result too large ({len(payload)} bytes > {_MAX_RESULT_PICKLE_BYTES}); "
                "a node likely stashed a large object in tracking"
            )
        pipe_w.send_bytes(payload)
    except Exception as e:  # serialization or pipe write failure
        try:
            fallback = pickle.dumps({
                "task_id": task_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_s": result.get("duration_s", 0.0),
                "status": "failed",
                "error": f"failed to serialize result: {e}",
                "traceback": traceback.format_exc(),
                "needs_continuation": False,
                "tracking": {},
            })
            pipe_w.send_bytes(fallback)
        except Exception:
            pass
    finally:
        try:
            pipe_w.close()
        except Exception:
            pass


class DAG:
    def __init__(
        self,
        specs: list[NodeSpec],
        maintain_specs: list[MaintainSpec] | None = None,
    ):
        # Validate: duplicate ids.
        seen: dict[str, NodeSpec] = {}
        for s in specs:
            if s.id in seen:
                raise ValueError(f"Duplicate NodeSpec id: {s.id!r}")
            seen[s.id] = s

        # Validate deps: every referenced id must exist (forward references to
        # later-declared specs are fine), no node may depend on itself, and the
        # graph must be acyclic. Caught here so a bad graph fails fast at
        # construction rather than deadlocking the run loop.
        _validate_deps(seen)

        # Maintain specs: keyed by asset_id. An asset_id with no matching
        # NodeSpec is a no-op (warned but not fatal — keeps the runtime tolerant
        # of stale maintain entries after a download module is regenerated).
        maintain_by_id: dict[str, MaintainSpec] = {}
        for m in (maintain_specs or []):
            if m.asset_id in maintain_by_id:
                raise ValueError(f"Duplicate MaintainSpec asset_id: {m.asset_id!r}")
            maintain_by_id[m.asset_id] = m
            if m.asset_id not in seen:
                print(
                    f"[DAG] Warning: MaintainSpec for {m.asset_id!r} has no "
                    f"matching NodeSpec — entry will be ignored"
                )
        self._maintain: dict[str, MaintainSpec] = maintain_by_id

        self._specs: dict[str, NodeSpec] = seen
        self.state: dict[str, dict] = {}
        self._needs_continuation = False
        self._shutdown_requested = False
        self._deadline: float | None = None
        self._deadline_hit = False
        self.topology_hash = _topology_hash(specs)

        for spec in specs:
            self.state[spec.id] = {
                "id": spec.id,
                "kind": spec.kind,
                "deps": list(spec.deps),
                # Where the fetch/transform fn was def'd — for debugging.
                # functools.wraps preserves this through decorators (@tenacity.retry etc).
                # SQL nodes have no fn; the runtime executor is their source.
                "source_module": (
                    "subsets_utils.sql_transform" if isinstance(spec, SqlNodeSpec)
                    else getattr(spec.fn, "__module__", None)
                ),
                "status": "pending",
                "started_at": None,
                "finished_at": None,
                "duration_s": None,
                "error": None,
                "raw_reads": [],
                "raw_writes": [],
                "subsets_reads": [],
                "materializations": [],
            }

        # Each main.py invocation is a fresh DAG run. Cross-invocation resume
        # was removed because it caused log/run.json to lie about which nodes
        # actually ran THIS invocation — the harness probe and the orchestrator
        # disagreed on where to find raw, and the agent saw phantom failures.
        # Per-asset idempotency now flows through state's `_metadata.code_hash`
        # rolled by record_completion after each successful run.

        # Per-spec code hashes:
        #   self._current_hashes[id]  — what THIS orchestrator computes for the
        #                                spec right now; stamped into state on
        #                                success. Audit-trail only — never
        #                                drives a skip/run decision by itself.
        #   self._expected_hashes     — what the harness expects (read from
        #                                $HARNESS_EXPECTED_HASHES_FILE). Drives
        #                                the bypass-maintain decision below.
        #                                In prod the env var is unset → None →
        #                                no bypass logic → behavior identical
        #                                to today.
        self._current_hashes: dict[str, str | None] = {}
        for spec in specs:
            if isinstance(spec, SqlNodeSpec):
                # The SQL text is the node's whole body — hash it directly.
                self._current_hashes[spec.id] = compute_sql_spec_hash(spec.sql)
                continue
            try:
                import inspect as _inspect
                src_file = _inspect.getsourcefile(spec.fn)
            except (TypeError, OSError):
                src_file = None
            if src_file:
                fence = [Path(src_file).parents[1]] if Path(src_file).parents else []
            else:
                fence = []
            self._current_hashes[spec.id] = compute_spec_hash(
                src_file, getattr(spec.fn, "__name__", ""),
                fence_dirs=fence,
            )

        hashes_file = os.environ.get("HARNESS_EXPECTED_HASHES_FILE")
        self._expected_hashes: dict[str, str | None] | None = None
        if hashes_file:
            try:
                self._expected_hashes = json.loads(Path(hashes_file).read_text())
                print(f"[DAG] expected hashes loaded ({len(self._expected_hashes)} specs) from {hashes_file}")
            except (OSError, json.JSONDecodeError) as e:
                print(f"[DAG] WARN: failed to read {hashes_file}: {e} — proceeding without hash gate")
                self._expected_hashes = None

    # =========================================================================
    # Order
    # =========================================================================

    def _execution_order(self) -> list[NodeSpec]:
        """Return specs topologically ordered by their deps, breaking ties in
        declaration order. Deps are validated acyclic at construction, so
        Kahn's algorithm always drains every node."""
        specs = list(self._specs.values())
        decl_index = {s.id: i for i, s in enumerate(specs)}
        indeg = {s.id: len(s.deps) for s in specs}
        dependents: dict[str, list[str]] = {s.id: [] for s in specs}
        for s in specs:
            for d in s.deps:
                dependents[d].append(s.id)

        ready = [s.id for s in specs if indeg[s.id] == 0]
        ordered: list[str] = []
        while ready:
            # Pop the earliest-declared ready node for a stable, deterministic
            # order among nodes with no remaining dependency.
            ready.sort(key=lambda sid: decl_index[sid])
            nid = ready.pop(0)
            ordered.append(nid)
            for dep_id in dependents[nid]:
                indeg[dep_id] -= 1
                if indeg[dep_id] == 0:
                    ready.append(dep_id)

        # Acyclicity is enforced at construction; this is a belt-and-suspenders
        # guard in case specs were mutated after __init__.
        if len(ordered) != len(specs):
            stuck = sorted(set(self._specs) - set(ordered))
            raise ValueError(f"NodeSpec dependency cycle among: {stuck}")
        return [self._specs[i] for i in ordered]

    # =========================================================================
    # Maintain
    # =========================================================================

    def _apply_maintain_skips(self, order: list[NodeSpec]) -> None:
        """Evaluate MaintainSpec.check() for each in-scope NodeSpec and mark
        fresh assets as done up-front (skipping the subprocess spawn).

        FORCE_REFRESH=1 (or true/yes) env bypasses all checks. A check that
        raises is treated as "not fresh" and logged — never lose a fetch over
        a buggy maintain check.

        Code-hash gate (dev-side activation only): when the harness writes
        $HARNESS_EXPECTED_HASHES_FILE and a spec's stored `_metadata.code_hash`
        on disk differs from the harness-expected hash, MaintainSpec is
        bypassed entirely for that spec — it stays `pending` and will run.
        In prod the env var is absent → this branch is no-op → behavior
        matches today.

        Only operates on `pending` nodes; "done" from resume/DAG_TARGET is
        left alone (already-done is already-fresh by definition)."""
        force = os.environ.get("FORCE_REFRESH", "").strip().lower() in ("1", "true", "yes")
        if force:
            print("[DAG] FORCE_REFRESH set — maintain skips bypassed")
            return

        forced_by_hash = 0
        skipped = 0
        for spec in order:
            if self.state[spec.id]["status"] != "pending":
                continue

            if self._expected_hashes is not None:
                stored = None
                try:
                    raw = _load_state_raw(spec.id)
                    meta = raw.get("_metadata") if isinstance(raw.get("_metadata"), dict) else None
                    stored = meta.get("code_hash") if meta else None
                except Exception:
                    stored = None
                expected = self._expected_hashes.get(spec.id)
                if stored != expected:
                    short_s = (stored or "")[:7] or "—"
                    short_e = (expected or "")[:7] or "—"
                    print(f"[DAG] {spec.id} forcing re-run (hash mismatch: {short_s}→{short_e})")
                    forced_by_hash += 1
                    continue  # leave pending; do NOT run maintain check

            maintain = self._maintain.get(spec.id)
            if maintain is None:
                continue
            try:
                fresh = bool(maintain.check(spec.id))
            except Exception as e:
                print(
                    f"[DAG] {spec.id} maintain check raised "
                    f"{type(e).__name__}: {e} — treating as not fresh"
                )
                continue
            if fresh:
                self.state[spec.id]["status"] = "done"
                self.state[spec.id]["error"] = None
                self.state[spec.id]["skipped_fresh"] = True
                self.state[spec.id]["maintain_description"] = maintain.description
                print(f"[DAG] {spec.id} fresh per maintain — {maintain.description}")
                skipped += 1
        if skipped:
            self.save_state()
            print(f"[DAG] Maintain: {skipped} asset(s) skipped as fresh")
        if forced_by_hash:
            print(f"[DAG] Code-hash gate: {forced_by_hash} asset(s) forced to re-run")

    # =========================================================================
    # Execution
    # =========================================================================

    def _spawn_task(self, spec: NodeSpec):
        """Fork a child process to run one node. Returns (proc, pipe_r).

        The supervisor closes its copy of the pipe write-end after fork so the
        read-end sees a clean EOF if the child dies without sending. The child
        inherits the read-end too but never uses it; that's harmless.
        """
        pipe_r, pipe_w = _MP_CTX.Pipe(duplex=False)
        if isinstance(spec, SqlNodeSpec):
            # SQL nodes have no connector fn — the runtime owns execution. The
            # runner is a top-level importable and the spec (id/deps/sql,
            # fn=None) pickles cleanly, so spawn-context is satisfied.
            from .sql_transform import run_sql_node
            fn, args = run_sql_node, (spec,)
        else:
            fn, args = spec.fn, (spec.id,)
        proc = _MP_CTX.Process(
            target=_child_entrypoint,
            args=(fn, args, spec.id, pipe_w),
            name=f"node:{spec.id}",
        )
        proc.start()
        # After fork, the child holds its own ref to pipe_w. The supervisor
        # must drop its copy so the pipe closes cleanly on child exit.
        pipe_w.close()
        return proc, pipe_r

    def _collect_result(self, proc: multiprocessing.Process, pipe_r) -> dict:
        """Join a child proc and read the result dict it sent. If the child
        died before sending (OOM SIGKILL, segfault, etc.), synthesize a failure
        result based on its exit code."""
        proc.join()

        result: dict | None = None
        if pipe_r.poll():
            try:
                result = pickle.loads(pipe_r.recv_bytes())
            except Exception:
                result = None
        try:
            pipe_r.close()
        except Exception:
            pass

        if result is not None:
            return result

        # Child died without sending a result.
        exitcode = proc.exitcode
        if exitcode is None:
            error = "child still alive after join (should not happen)"
        elif exitcode < 0:
            try:
                signame = signal.Signals(-exitcode).name
            except (ValueError, AttributeError):
                signame = f"signal {-exitcode}"
            error = f"killed by {signame} (exitcode={exitcode}); likely OOM or external kill"
        else:
            error = f"child exited with code {exitcode} before sending result"

        now = datetime.now(timezone.utc).isoformat()
        return {
            "task_id": proc.name.split(":", 1)[-1] if ":" in proc.name else proc.name,
            "status": "failed",
            "error": error,
            "traceback": "",
            "started_at": now,
            "finished_at": now,
            "duration_s": 0.0,
            "needs_continuation": False,
            "tracking": {},
        }

    def _apply_result(self, task_id: str, result: dict) -> None:
        """Merge a child result dict into self.state and the tracking module."""
        task_state = self.state[task_id]
        task_state["status"] = result["status"]
        task_state["started_at"] = result.get("started_at")
        task_state["finished_at"] = result.get("finished_at")
        task_state["duration_s"] = result.get("duration_s")
        if result["status"] == "failed":
            task_state["error"] = result.get("error", "unknown")
            task_state["traceback"] = result.get("traceback", "")
        elif result.get("needs_continuation"):
            task_state["needs_continuation"] = True
            self._needs_continuation = True

        # Record completion: stamp `_metadata.code_hash` in the spec's state
        # file with the hash this orchestrator computed at init. Audit record
        # of "what code produced this state"; in dev the next harness run
        # diffs against this. Only on success — failures preserve the prior
        # state file unchanged. Runs even on needs_continuation (the spec
        # made progress under this code).
        if result["status"] == "done":
            try:
                record_completion(task_id, self._current_hashes.get(task_id))
            except Exception as e:
                # Don't let a state-write hiccup tank an otherwise successful node.
                print(f"[DAG] WARN: record_completion failed for {task_id}: {type(e).__name__}: {e}")

        # Merge child's run-record snapshot into the supervisor's record so
        # to_json() and _print_node_detail() see this node's I/O, HTTP, state.
        tracking.merge(result.get("tracking") or {})

    def run(self, targets: list[str] | None = None):
        """Execute all nodes in dependency order, each in its own forked
        subprocess. Writes run.json after every node.

        Args:
            targets: Optional list of spec kinds OR spec ids to run.

        Env vars:
            DAG_TARGET: Comma-separated target tokens. Each token matches against
              spec.kind first, falling back to exact spec.id match.
            DAG_ON_FAILURE: "crash" (default) or "continue".
            DAG_PARALLELISM: Max concurrent nodes (default 1 = sequential).
            DAG_DRAIN_TIMEOUT_S: Max seconds to wait for children on SIGTERM (default 8).
            DAG_MAX_CONSECUTIVE_FAILURES: In continue mode, halt after this many
              consecutive node failures (default 10). Resets on any success.
              Guards against thrashing when an entire run is broken (auth dead,
              R2 down, etc.). Ignored in crash mode.

        Behavior:
            - Each node runs in a fresh forked child; OS reclaims RSS on exit.
            - A node OOM (SIGKILL) only fails that node; the rest of the DAG
              continues unless DAG_ON_FAILURE=crash.
            - On a node returning True: marks needs_continuation, continues running.
            - On node failure with crash mode: drains in-flight tasks, then raises.
            - On node failure with continue mode: raises after all nodes complete,
              OR halts early if DAG_MAX_CONSECUTIVE_FAILURES is reached.
            - On SIGTERM: drains in-flight, marks remaining as failed, writes
              run.json with status="failed". No auto-retrigger from host kill.
            - The DAG class never calls sys.exit() — exit codes are runner.py's job.
        """
        clear_tracking()
        # Create http_requests.csv (header only) once, in this single process,
        # before any node forks — children only append rows. Per-invocation,
        # like output.log / memory.csv.
        tracking.init_http_log()

        on_failure = os.environ.get("DAG_ON_FAILURE", "crash")
        try:
            max_consec = max(1, int(os.environ.get("DAG_MAX_CONSECUTIVE_FAILURES", "10")))
        except ValueError:
            max_consec = 10
        try:
            parallelism = max(1, int(os.environ.get("DAG_PARALLELISM", "1")))
        except ValueError:
            parallelism = 1
        try:
            budget_s = float(os.environ.get("DAG_TIME_BUDGET", "0"))
        except ValueError:
            budget_s = 0.0
        if budget_s > 0:
            self._deadline = time.monotonic() + budget_s
            print(f"[DAG] Time budget: {budget_s/3600:.2f}h")
        env_targets = os.environ.get("DAG_TARGET")
        if env_targets:
            targets = [t.strip() for t in env_targets.split(",")]

        order = self._execution_order()

        if targets:
            target_set = set(targets)
            order = [
                s for s in order
                if s.kind in target_set or s.id in target_set
            ]
            if not order:
                print(f"[DAG] No nodes matched targets: {targets}")
                print(f"[DAG] Available kinds: "
                      f"{sorted({s.kind for s in self._specs.values()})}")
                self.save_state()
                return self
            # Mark all non-targeted nodes as "done". The semantic of
            # DAG_TARGET=<kind> is "run only this kind, assuming earlier kinds
            # already ran in a previous invocation." Non-targeted specs are
            # left out of `order` entirely; marking them done keeps the
            # manifest/status coherent (they aren't pending-but-never-run).
            targeted_ids = {s.id for s in order}
            for task_id, st in self.state.items():
                if task_id not in targeted_ids and st["status"] == "pending":
                    st["status"] = "done"
                    st["error"] = None
                    # No started_at/finished_at — these weren't run in this
                    # invocation; the timestamps document the run that did.

        # DAG_SKIP_DOWNLOAD=1 short-circuits all download-kind specs without
        # spawning subprocesses. The reprocess flow: pin RUN_ID to a prior run
        # whose raw is on disk, set DAG_SKIP_DOWNLOAD=1, and downstream specs
        # (transform/curate/…) execute against the existing raw. Works with or
        # without DAG_TARGET; when used alongside DAG_TARGET=transform the two
        # flags are redundant but harmless (DAG_TARGET already marks downloads
        # done via non-targeted handling above).
        if os.environ.get("DAG_SKIP_DOWNLOAD", "").strip().lower() in ("1", "true", "yes"):
            skip_count = 0
            for sid, spec in self._specs.items():
                if spec.kind == "download" and self.state[sid]["status"] == "pending":
                    self.state[sid]["status"] = "done"
                    self.state[sid]["error"] = None
                    self.state[sid]["skipped_download"] = True
                    skip_count += 1
            if skip_count:
                print(f"[DAG] DAG_SKIP_DOWNLOAD: marked {skip_count} download spec(s) as done (reprocess mode)")
                self.save_state()

        # Nodes already marked done at this point come from DAG_TARGET filtering
        # (non-targeted kinds) or DAG_SKIP_DOWNLOAD. Log them so output ordering
        # is consistent with subsequent run lines.
        for spec in order:
            if self.state[spec.id]["status"] == "done":
                print(f"[DAG] {spec.id} skipped (filtered)")

        # Maintain skips: evaluate per-asset freshness checks BEFORE any
        # subprocess spawn. A True result marks the NodeSpec done so dependents
        # (e.g. transforms) proceed without re-fetching. FORCE_REFRESH=1
        # bypasses all checks. We only evaluate maintain entries whose asset_id
        # is in the current `order` (DAG_TARGET may have filtered downloads
        # out — in that case the orchestrator already marked them done).
        if self._maintain:
            self._apply_maintain_skips(order)

        first_failure = None
        stop_submitting = False
        consecutive_failures = 0

        def propagate_blocked() -> None:
            """Mark any pending node whose dependency has failed as failed
            itself (it can never become ready), so the run terminates instead of
            spinning on un-runnable nodes. Cascades to fixpoint: blocking a node
            may block its own dependents. Does NOT set first_failure — the
            originating dependency already accounted for that.

            A dep that is `done` (incl. maintain-skipped, DAG_TARGET-filtered, or
            DAG_SKIP_DOWNLOAD-marked) is satisfied; only `failed` deps block."""
            changed = True
            while changed:
                changed = False
                for s in order:
                    st = self.state[s.id]
                    if st["status"] != "pending":
                        continue
                    bad = [d for d in s.deps if self.state[d]["status"] == "failed"]
                    if bad:
                        st["status"] = "failed"
                        st["error"] = f"skipped: dependency failed ({', '.join(bad)})"
                        st["skipped_blocked"] = True
                        print(f"[DAG] {s.id} blocked — dependency failed: {', '.join(bad)}")
                        changed = True
            self.save_state()

        def find_ready() -> list[NodeSpec]:
            """Return pending specs whose every dep has completed (`done`), in
            declaration order. A node with no deps is ready immediately."""
            return [
                s for s in order
                if self.state[s.id]["status"] == "pending"
                and all(self.state[d]["status"] == "done" for d in s.deps)
            ]

        # Each node runs in its own forked subprocess so memory is reclaimed
        # between nodes. in_flight maps a live Process to its (task_id, pipe_r).
        in_flight: dict[multiprocessing.Process, tuple[str, object]] = {}

        # SIGTERM policy depends on DAG_ON_FAILURE:
        #
        # - "continue": ignore SIGTERM entirely. GitHub Actions sometimes sends
        #   SIGTERM to the whole step when a child OOMs and the host briefly
        #   thrashes — but the OOM killer already reaped the offending child,
        #   so we can keep going. If GH really wants us dead it sends SIGKILL
        #   ~10s after SIGTERM, which we cannot catch, and the step dies hard.
        #   That is acceptable: save_state runs after every node so at most a
        #   few seconds of progress is lost.
        #
        # - "crash" (default): drain in-flight, mark pending, exit. Used by
        #   callers who want a single failure to halt the run cleanly.
        prior_handler = signal.getsignal(signal.SIGTERM)
        ignore_sigterm = (on_failure == "continue")

        def _on_sigterm(signum, frame):
            nonlocal stop_submitting
            if ignore_sigterm:
                print("[DAG] Received SIGTERM (ignored — DAG_ON_FAILURE=continue)")
                return
            print("[DAG] Received SIGTERM, draining in-flight nodes...")
            stop_submitting = True
            self._shutdown_requested = True

        try:
            signal.signal(signal.SIGTERM, _on_sigterm)
        except ValueError:
            # signal.signal can only be called from the main thread; if we're
            # in a worker thread we just skip — the supervisor is normally main.
            pass

        def submit_more():
            nonlocal stop_submitting
            if stop_submitting:
                return
            if self._deadline is not None and time.monotonic() >= self._deadline:
                if not self._deadline_hit:
                    print("[DAG] Time budget exhausted — stop scheduling, will request continuation")
                    self._deadline_hit = True
                    self._needs_continuation = True
                stop_submitting = True
                return
            for spec in find_ready():
                if len(in_flight) >= parallelism:
                    return
                # Reserve the slot before fork so the next find_ready() doesn't
                # see this node as pending and re-spawn it.
                self.state[spec.id]["status"] = "running"
                self.state[spec.id]["started_at"] = datetime.now(timezone.utc).isoformat()
                print(f"[DAG] Running {spec.id}...")
                proc, pipe_r = self._spawn_task(spec)
                in_flight[proc] = (spec.id, pipe_r)

        def collect_one(proc: multiprocessing.Process) -> dict:
            """Pop a finished proc, collect its result, apply, save_state."""
            task_id, pipe_r = in_flight.pop(proc)
            result = self._collect_result(proc, pipe_r)
            self._apply_result(task_id, result)
            self.save_state()
            return result

        try:
            submit_more()

            while in_flight:
                # Wait for any child to exit. We poll on a timeout so the
                # SIGTERM-set stop_submitting flag is observed promptly.
                sentinels = [p.sentinel for p in in_flight]
                ready = multiprocessing.connection.wait(sentinels, timeout=1.0)

                # Map sentinels back to processes. multiprocessing.connection.wait
                # returns the sentinel objects; we match by identity.
                done_procs = [p for p in list(in_flight) if p.sentinel in ready]
                batch_had_failure = False
                for proc in done_procs:
                    task_id, _ = in_flight[proc]
                    result = collect_one(proc)

                    if result["status"] == "done":
                        cont_msg = " (needs continuation)" if result.get("needs_continuation") else ""
                        duration = result.get("duration_s") or 0.0
                        print(f"[DAG] {task_id} done ({duration:.1f}s){cont_msg}")
                        if os.environ.get("DAG_VERBOSE") == "1":
                            self._print_node_detail(task_id)
                        consecutive_failures = 0
                    else:
                        batch_had_failure = True
                        print(f"[DAG] {task_id} failed: {result.get('error', 'unknown')}")
                        if first_failure is None:
                            first_failure = result
                        consecutive_failures += 1
                        if on_failure == "crash":
                            stop_submitting = True
                        elif consecutive_failures >= max_consec:
                            print(f"[DAG] Halting: {consecutive_failures} consecutive failures "
                                  f"(DAG_MAX_CONSECUTIVE_FAILURES={max_consec}). "
                                  f"Likely a systemic issue — fix and rerun.")
                            stop_submitting = True

                # A failure orphans its (transitive) dependents — they can never
                # be ready. Mark them failed so the loop terminates instead of
                # spinning with an empty find_ready() over un-runnable pendings.
                if batch_had_failure:
                    propagate_blocked()

                if self._shutdown_requested:
                    break

                submit_more()

                # Deadline watchdog. submit_more() flips _deadline_hit (and
                # _needs_continuation) once the time budget is spent, but a
                # single long-running node would otherwise keep us blocked in
                # this loop until GHA's hard timeout SIGKILLs the whole job —
                # leaving run.json unfinalized, logs un-uploaded, and no
                # retrigger (the 2026-06-01 mass-failure mode). Break out now so
                # the drain block below force-kills the stragglers and we exit
                # cleanly as needs_continuation with ~10 min of margin to spare.
                if self._deadline_hit and in_flight:
                    print(f"[DAG] Deadline watchdog: interrupting {len(in_flight)} "
                          f"in-flight node(s) to finalize continuation")
                    break

            # Drain any remaining in-flight children after a shutdown signal OR a
            # budget-deadline interrupt. There are no connector-side teardown
            # hooks, so this grace window is NOT for cleanup — it's a last chance
            # to collect a real result from any child that happens to finish in
            # the next few seconds, after which we hard-kill the rest.
            if in_flight:
                drain_timeout = float(os.environ.get("DAG_DRAIN_TIMEOUT_S", "8"))
                deadline = time.monotonic() + drain_timeout
                while in_flight and time.monotonic() < deadline:
                    remaining = max(0.0, deadline - time.monotonic())
                    sentinels = [p.sentinel for p in in_flight]
                    ready = multiprocessing.connection.wait(sentinels, timeout=remaining)
                    for proc in [p for p in list(in_flight) if p.sentinel in ready]:
                        collect_one(proc)

                # Anyone still alive: SIGTERM, then SIGKILL.
                for proc in list(in_flight):
                    task_id, pipe_r = in_flight[proc]
                    print(f"[DAG] {task_id}: sending SIGTERM to child...")
                    try:
                        proc.terminate()
                        proc.join(timeout=5)
                    except Exception:
                        pass
                    if proc.is_alive():
                        print(f"[DAG] {task_id}: SIGKILL")
                        try:
                            proc.kill()
                            proc.join(timeout=2)
                        except Exception:
                            pass
                    # Account for the force-killed node — two distinct cases.
                    in_flight.pop(proc, None)
                    if self._deadline_hit:
                        # Budget-deadline interrupt: the node didn't fail, it ran
                        # out of wall-clock. Reset it to "pending" so
                        # _overall_status() reports needs_continuation — a node
                        # left as "failed" would force the whole run to
                        # status="failed", which suppresses the retrigger and
                        # re-creates the original bug. The continuation run
                        # re-derives it fresh via maintain-skip freshness; any
                        # partial raw is simply re-fetched. Deliberately does NOT
                        # set first_failure (no exception, no exit-1).
                        st = self.state[task_id]
                        st["status"] = "pending"
                        st["error"] = None
                        st["finished_at"] = None
                        st["interrupted_at_deadline"] = True
                        print(f"[DAG] {task_id}: interrupted at deadline → pending "
                              f"(continuation will resume it)")
                        self.save_state()
                    else:
                        # Shutdown (SIGTERM/crash) kill: a host-initiated stop is a
                        # real failure — no auto-retrigger from host kill.
                        now = datetime.now(timezone.utc).isoformat()
                        self._apply_result(task_id, {
                            "task_id": task_id,
                            "status": "failed",
                            "error": "killed during shutdown",
                            "traceback": "",
                            "started_at": self.state[task_id].get("started_at") or now,
                            "finished_at": now,
                            "duration_s": 0.0,
                            "needs_continuation": False,
                            "tracking": {},
                        })
                        self.save_state()
                        if first_failure is None:
                            first_failure = self.state[task_id]
        finally:
            # Restore prior signal handler (mostly relevant for tests / repeated runs).
            try:
                signal.signal(signal.SIGTERM, prior_handler)
            except (ValueError, TypeError):
                pass

        # Final state save with overall status
        self.save_state()

        if first_failure is not None:
            failed_id = first_failure.get("id") or first_failure.get("task_id") or "unknown"
            raise RuntimeError(
                f"[DAG] {failed_id} failed: {first_failure.get('error', 'unknown')}"
            )

        return self

    # =========================================================================
    # Serialization
    # =========================================================================

    def _print_node_detail(self, task_id: str) -> None:
        """Print per-node data flow (raw_writes, raw_reads, materializations).

        Called by run() after a successful node when DAG_VERBOSE=1.
        Reads from the tracking module (filled by io.py + delta.py during the node).
        """
        writes = get_assets_by_writer(task_id)
        reads = get_reads_by_task(task_id)

        raw_writes = [w for w in writes if w.startswith("raw/") or "/raw/" in w]
        materializations = []
        for w in writes:
            if w.startswith("subsets/"):
                name = w.replace("subsets/", "")
                vi = get_asset_version(w)
                if vi:
                    materializations.append(f"{name} (v{vi['version']})")
                else:
                    materializations.append(name)
        raw_reads = [r for r in reads if r.startswith("raw/") or "/raw/" in r]

        for label, vals in (
            ("raw_writes", raw_writes),
            ("raw_reads", raw_reads),
            ("materializations", materializations),
        ):
            if vals:
                print(f"      {label + ':':<18}{', '.join(vals)}")

    def _overall_status(self) -> str:
        statuses = [n["status"] for n in self.state.values()]
        if "failed" in statuses:
            return "failed"
        if "running" in statuses:
            return "running"
        if "pending" in statuses:
            return "needs_continuation" if self._needs_continuation else "running"
        return "needs_continuation" if self._needs_continuation else "done"

    def to_json(self) -> dict:
        """Build the run.json payload from current state + tracking data."""
        nodes_with_io = []
        for node_state in self.state.values():
            task_id = node_state["id"]
            writes = get_assets_by_writer(task_id)
            reads = get_reads_by_task(task_id)

            raw_writes = [w for w in writes if w.startswith("raw/") or "/raw/" in w]
            materializations = []
            for w in writes:
                if w.startswith("subsets/"):
                    name = w.replace("subsets/", "")
                    vi = get_asset_version(w)
                    if vi:
                        materializations.append({"name": name, **vi})
                    else:
                        materializations.append({"name": name})
            raw_reads = [r for r in reads if r.startswith("raw/") or "/raw/" in r]
            subsets_reads = [
                r.replace("subsets/", "") for r in reads if r.startswith("subsets/")
            ]

            # Merge tracking-derived fields with whatever is in node_state
            merged = {**node_state}
            if raw_writes or not merged.get("raw_writes"):
                merged["raw_writes"] = raw_writes or merged.get("raw_writes", [])
            if raw_reads or not merged.get("raw_reads"):
                merged["raw_reads"] = raw_reads or merged.get("raw_reads", [])
            if subsets_reads or not merged.get("subsets_reads"):
                merged["subsets_reads"] = subsets_reads or merged.get("subsets_reads", [])
            if materializations or not merged.get("materializations"):
                merged["materializations"] = materializations or merged.get("materializations", [])

            # Full state-change list, carried into this process by the child's
            # snapshot/merge. (Per-node HTTP {count, error_count} is NOT set
            # here — it's derived from http_requests.csv at finalize by
            # runner._stamp_run_enrichments and injected into dag.nodes[].http.)
            state_changes = tracking.get_state_changes(task_id)
            if state_changes or not merged.get("state_changes"):
                merged["state_changes"] = state_changes or merged.get("state_changes", [])
            nodes_with_io.append(merged)

        return {
            "run_id": os.environ.get("RUN_ID", "unknown"),
            "connector": os.environ.get("CONNECTOR_NAME") or Path.cwd().name,
            "status": self._overall_status(),
            "topology_hash": self.topology_hash,
            "started_at": min(
                (n.get("started_at") for n in self.state.values() if n.get("started_at")),
                default=None,
            ),
            "finished_at": max(
                (n.get("finished_at") for n in self.state.values() if n.get("finished_at")),
                default=None,
            ),
            "dag": {
                "nodes": nodes_with_io,
                # One edge per declared dependency: {"from": dep, "to": node}.
                "edges": [
                    {"from": d, "to": spec.id}
                    for spec in self._specs.values()
                    for d in spec.deps
                ],
                "total_duration_s": sum(
                    n.get("duration_s") or 0 for n in self.state.values()
                ),
            },
        }

    def save_state(self):
        """Write run.json to LOG_DIR. Called after each node, can also be called explicitly."""
        log_dir = os.environ.get("LOG_DIR")
        if not log_dir:
            return  # Local dev without runner — skip persistence
        path = Path(log_dir) / "run.json"
        # Preserve invocations array from prior state if present
        existing = _load_run_state(Path(log_dir)) or {}
        payload = self.to_json()
        if "invocations" in existing:
            payload["invocations"] = existing["invocations"]
        if "git_hash" in existing:
            payload["git_hash"] = existing["git_hash"]
        _atomic_write_json(path, payload)


# =============================================================================
# Node loading
# =============================================================================

def load_nodes(nodes_dir: Path | str | None = None) -> DAG:
    """Discover all node files in `nodes_dir` and assemble their *_SPECS lists.

    Each node module exposes one or more module-level constants whose names end
    with `_SPECS` (e.g. `DOWNLOAD_SPECS`, `TRANSFORM_SPECS`). The value must be
    a homogeneous list of either NodeSpec OR MaintainSpec instances. NodeSpecs
    are unioned into the DAG; MaintainSpecs are routed to the freshness engine
    (evaluated pre-spawn to skip fresh assets). Duplicate ids and unknown deps
    fail at construction.
    """
    if nodes_dir is None:
        nodes_dir = Path.cwd() / "src" / "nodes"
    elif isinstance(nodes_dir, str):
        nodes_dir = Path(nodes_dir)

    print(f"Loading nodes from: {nodes_dir}")

    all_specs: list[NodeSpec] = []
    all_maintain: list[MaintainSpec] = []

    if not nodes_dir.exists():
        print(f"Warning: nodes directory not found: {nodes_dir}")
        return DAG(all_specs, all_maintain)

    top_level = sorted(nodes_dir.glob("*.py"))
    nested = sorted(
        f for f in nodes_dir.glob("*/*.py")
        if f.parent.name != "__pycache__"
    )
    node_files = top_level + nested

    # Re-imported *_SPECS lists share the same Python object across modules
    # (e.g. transform module does `from nodes.<download> import DOWNLOAD_SPECS`
    # to keep ids in sync). dir() can't tell defined-here from re-imported,
    # so we dedupe by list identity — first sighting wins. Alphabetical iteration
    # means the defining module is normally encountered before any re-exporter.
    seen_spec_lists: set[int] = set()

    for node_file in node_files:
        if node_file.name.startswith("_"):
            continue

        rel = node_file.relative_to(nodes_dir).with_suffix("")
        module_name = "nodes." + ".".join(rel.parts)

        try:
            if module_name in sys.modules:
                module = sys.modules[module_name]
            else:
                spec = importlib.util.spec_from_file_location(module_name, node_file)
                if spec is None or spec.loader is None:
                    print(f"Warning: Could not load spec for {node_file}")
                    continue
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)

            # Collect every *_SPECS constant. Homogeneous lists of NodeSpec
            # go to the DAG; homogeneous lists of MaintainSpec go to the
            # maintain engine. Mixed/empty lists are skipped silently.
            # Re-imported lists are deduped by identity — see seen_spec_lists.
            collected_in_module = 0
            for attr_name in dir(module):
                if not attr_name.endswith("_SPECS") or attr_name.startswith("_"):
                    continue
                value = getattr(module, attr_name, None)
                if not isinstance(value, list) or not value:
                    continue
                if id(value) in seen_spec_lists:
                    continue  # already collected from the defining module
                seen_spec_lists.add(id(value))
                if all(isinstance(s, NodeSpec) for s in value):
                    all_specs.extend(value)
                    collected_in_module += len(value)
                elif all(isinstance(s, MaintainSpec) for s in value):
                    all_maintain.extend(value)
                    collected_in_module += len(value)

            if collected_in_module == 0:
                print(f"Warning: {node_file.name} has no *_SPECS constants — skipping")

        except Exception as e:
            print(f"Error loading {node_file.name}: {e}")
            raise

    print(f"Loaded {len(all_specs)} nodes, {len(all_maintain)} maintain specs")
    return DAG(all_specs, all_maintain)
