"""NodeSpec: the unit the runtime DAG executes.
SqlNodeSpec: a transform node whose body is one DuckDB SQL query.
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

    `deps` lists the ids of other NodeSpecs that must finish successfully
    before this node is spawned. Default `()` means independent (the historical
    behavior — every node ready immediately, run in declaration order). Pass a
    list of ids to gate a node behind others (e.g. a transform that reads what a
    download wrote). The DAG topologically orders nodes by their deps; a node
    whose dependency fails or is blocked is itself marked failed rather than
    run. Unknown dep ids and dependency cycles fail at DAG construction. Stored
    as a tuple so the frozen spec stays hashable; you may pass any iterable.
    """
    id: str
    fn: Callable
    kind: str = "download"
    deps: tuple[str, ...] = ()

    def __post_init__(self):
        # Normalize deps to a tuple so the frozen dataclass stays hashable and
        # immutable while still accepting the ergonomic `deps=[...]` list form.
        # Guard against a bare string, which tuple() would silently shred into
        # one entry per character.
        if isinstance(self.deps, str):
            raise TypeError(
                f"NodeSpec {self.id!r}: deps must be a list/tuple of ids, "
                f"not a str ({self.deps!r})"
            )
        if not isinstance(self.deps, tuple):
            object.__setattr__(self, "deps", tuple(self.deps))


_TRANSFORM_SUFFIX = "-transform"


@dataclass(frozen=True)
class SqlNodeSpec(NodeSpec):
    """A transform node defined by one DuckDB SQL query instead of a Python fn.

    Contract: raw asset(s) on R2 in → SQL → ONE Delta table out (overwrite).
    The runtime (not the connector) owns execution: each id in `deps` is
    registered as a DuckDB view named after that dep (quote it in SQL — ids
    contain dashes: `FROM "slug-entity"`), `sql` runs against those views, and
    the result is written with `overwrite()` to the Delta table named `id`
    minus the "-transform" suffix. A query that yields zero rows fails the
    node — the transform doubles as the correctness gate on raw.

    `fn` is inherited but unused; it must stay None. `kind` is pinned to
    "transform". `deps` must name at least one upstream node. Raw assets read
    this way must be SQL-readable: parquet, ndjson/json, or csv (compressed
    variants included) — anything else needs normalizing in the download fn.
    """
    fn: Callable | None = None
    kind: str = "transform"
    sql: str = ""

    def __post_init__(self):
        super().__post_init__()
        if self.fn is not None:
            raise TypeError(
                f"SqlNodeSpec {self.id!r}: fn must not be set — the runtime "
                "executes `sql`; use a plain NodeSpec for Python transforms"
            )
        if not isinstance(self.sql, str) or not self.sql.strip():
            raise ValueError(f"SqlNodeSpec {self.id!r}: sql must be a non-empty string")
        if self.kind != "transform":
            raise ValueError(
                f"SqlNodeSpec {self.id!r}: kind must be 'transform' (got {self.kind!r})"
            )
        if not self.deps:
            raise ValueError(
                f"SqlNodeSpec {self.id!r}: deps must name at least one upstream "
                "node — its raw asset is the SQL's input view"
            )

    @property
    def table(self) -> str:
        """Delta table name this node publishes: id minus the '-transform' suffix."""
        if self.id.endswith(_TRANSFORM_SUFFIX):
            return self.id[: -len(_TRANSFORM_SUFFIX)]
        return self.id


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
