"""GitHub Actions platform glue for the runner.

Isolates the GitHub-specific bits of supervision — the self-retrigger
workflow dispatch and the $GITHUB_OUTPUT handshake — so runner.py's core
(spawn, stdout capture, memory profiling, SIGTERM policy, exit-code contract)
stays platform-agnostic.
"""

import json
import os

from .config import get_connector_name


def write_resolved_run_id(run_id: str) -> None:
    """Export the resolved RUN_ID to $GITHUB_OUTPUT.

    Lets the workflow's retrigger step pass the id back as the `run_id` input —
    preserving DAG resume across retriggers even when the first invocation was
    started without one. No-op off GitHub Actions.
    """
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if not gh_output:
        return
    try:
        with open(gh_output, "a") as f:
            f.write(f"resolved_run_id={run_id}\n")
    except OSError:
        pass


def maybe_retrigger(run_id: str) -> bool:
    """Dispatch our own workflow with the same run_id, so a long ingest
    survives GHA's 6h job cap without requiring per-workflow retrigger steps.

    Why a PAT: GITHUB_TOKEN cannot dispatch workflows on its own repo (GHA
    blocks recursion). A user/fine-grained PAT with `actions:write` does.

    Returns True if the dispatch was accepted, False otherwise (missing PAT/
    repo, or an API error — caller falls back to exit 2).
    """
    pat = os.environ.get("GH_RETRIGGER_PAT")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not pat:
        print("[runner] GH_RETRIGGER_PAT not set; cannot self-retrigger")
        return False
    if not repo:
        print("[runner] GITHUB_REPOSITORY not set; cannot self-retrigger")
        return False

    ref = os.environ.get("GITHUB_REF_NAME") or "main"
    url = f"https://api.github.com/repos/{repo}/actions/workflows/run.yml/dispatches"
    # The single-repo connectors workflow requires `slug`; carry the DAG
    # params too so the continuation run resumes with identical scope.
    inputs = {"run_id": run_id, "slug": get_connector_name()}
    for env_key, input_key in (("DAG_TARGET", "dag_target"),
                               ("DAG_ON_FAILURE", "dag_on_failure")):
        val = os.environ.get(env_key)
        if val:
            inputs[input_key] = val
    body = json.dumps({"ref": ref, "inputs": inputs}).encode()

    import urllib.request
    import urllib.error
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Authorization": f"Bearer {pat}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status in (200, 204):
                print(f"[runner] Self-retriggered run.yml on {repo}@{ref} with run_id={run_id}")
                return True
            print(f"[runner] Self-retrigger HTTP {resp.status}")
            return False
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:500]
        print(f"[runner] Self-retrigger HTTP {e.code}: {body}")
        return False
    except Exception as e:
        print(f"[runner] Self-retrigger failed: {e}")
        return False
