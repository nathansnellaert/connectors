"""Post-DAG health tests — run model-authored `test_*` invariants in-connector.

The harness's download step has the model write a `tests_download.py` file
into the connector root. After the DAG finishes, `main.py` calls
`run_health_tests()`, which runs those `test_*` functions *here* — inside the
connector run, where data access resolves identically whether the run is
local or on GitHub Actions (R2). Results are merged into `LOG_DIR/run.json`
under a `tests` key; the harness reads them back from there.
"""
from __future__ import annotations

import importlib.util
import inspect
import json
import os
import traceback
from pathlib import Path


def _run_test_file(tests_file: Path, available: dict) -> dict:
    """Import a tests file, run each `test_*` with signature-filtered kwargs."""
    spec = importlib.util.spec_from_file_location(f"_health_{tests_file.stem}", tests_file)
    if spec is None or spec.loader is None:
        return {"__import_error__": {"passed": False, "error": "cannot load tests file"}}
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        return {"__import_error__": {
            "passed": False,
            "error": f"{type(e).__name__}: {e}",
            "traceback": traceback.format_exc(),
        }}

    results: dict[str, dict] = {}
    for name in sorted(dir(module)):
        if not name.startswith("test_"):
            continue
        fn = getattr(module, name)
        if not callable(fn):
            continue
        try:
            params = inspect.signature(fn).parameters
            kwargs = {k: v for k, v in available.items() if k in params}
        except (TypeError, ValueError):
            kwargs = {}
        try:
            fn(**kwargs)
            results[name] = {"passed": True, "error": None}
        except AssertionError as e:
            results[name] = {"passed": False, "error": str(e) or "assertion failed (no message)"}
        except Exception as e:
            results[name] = {
                "passed": False,
                "error": f"{type(e).__name__}: {e}",
                "traceback": traceback.format_exc(),
            }
    return results


def run_health_tests(connector_root) -> None:
    """Run the connector's model-authored health tests, post-DAG.

    No-op unless `run.json` exists with status `done` (a partial run would
    assert against incomplete data) and a tests file is present. Picks
    `tests_transform.py` when `DAG_TARGET=transform`, else `tests_download.py`.
    Writes results into `run.json` under `tests`.
    """
    log_dir = os.environ.get("LOG_DIR")
    if not log_dir:
        return
    run_json = Path(log_dir) / "run.json"
    if not run_json.is_file():
        return
    try:
        run = json.loads(run_json.read_text())
    except (OSError, json.JSONDecodeError):
        return
    if run.get("status") != "done":
        return

    target = os.environ.get("DAG_TARGET", "").strip()
    fname = "tests_transform.py" if target == "transform" else "tests_download.py"
    tests_file = Path(connector_root) / fname
    if not tests_file.is_file():
        return

    spec_ids = [
        n.get("id") for n in (run.get("dag", {}).get("nodes") or [])
        if isinstance(n, dict) and n.get("id")
    ]
    available = {"slug": os.environ.get("CONNECTOR_NAME", ""), "spec_ids": spec_ids}
    if target == "transform":
        suffix = "-transform"
        available["subset_ids"] = [i[:-len(suffix)] for i in spec_ids if i.endswith(suffix)]
    results = _run_test_file(tests_file, available)

    run["tests"] = results
    try:
        run_json.write_text(json.dumps(run, indent=2))
    except OSError:
        return
    passed = sum(1 for r in results.values() if r.get("passed"))
    print(f"[health tests] {passed}/{len(results)} passed")
