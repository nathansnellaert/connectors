"""NodeSpec: the unit the runtime DAG executes.
MaintainSpec: freshness policy consumed by the orchestrator pre-spawn."""
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class NodeSpec:
    """A single DAG node.

    `fn` must be importable (top-level function or method); closures and
    lambdas fail under spawn-context subprocess execution. The runtime calls
    it as `fn(id)` — a node's only input is its own id, which is also the
    asset name it writes. Nodes do not pass state to one another.

    `id` must be globally unique within a connector's loaded specs.
    `kind` is matched against DAG_TARGET when filtering (e.g. "download",
    "transform"). It also routes per-stage status in manifests.
    """
    id: str
    fn: Callable
    kind: str = "download"


@dataclass(frozen=True)
class MaintainSpec:
    """Freshness policy for one download asset.

    Consumed by the orchestrator before the DAG runs: if `check(asset_id)`
    returns True the corresponding NodeSpec is marked done up-front and its
    subprocess never spawns. `FORCE_REFRESH=1` env bypasses all checks.

    `asset_id` must match a download NodeSpec.id in the same connector.
    `description` is human-readable cadence + basis (UI/audit). Include the
    citation inline — "Every TARGET business day @ 16:00 CET (per <URL>)",
    "Updated weekly, observed via Last-Modified header", or
    "Likely monthly based on dataset nature (inferred — no published cadence)".
    `check(asset_id) -> bool` returns True when the asset is fresh enough to
    skip. Most often: `lambda aid: raw_asset_exists(aid, ext, max_age_days=N)`.
    """
    asset_id: str
    description: str
    check: Callable[[str], bool]
