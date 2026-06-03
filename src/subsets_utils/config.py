"""Configuration and environment utilities.

Single source of truth for paths, environment detection, and storage options.
The same code runs both local and cloud (R2) modes — the only difference is
which URI a path-builder returns.
"""

import os
from pathlib import Path


# =============================================================================
# Environment Detection
# =============================================================================

def is_cloud() -> bool:
    """Check if running in cloud mode (CI environment)."""
    return os.environ.get('CI', '').lower() == 'true'


def get_connector_name() -> str:
    """Get current connector name. Auto-detects from cwd if not set."""
    return os.environ.get('CONNECTOR_NAME') or Path.cwd().name


# =============================================================================
# Directory Configuration
# =============================================================================

def get_data_dir() -> str:
    """Root directory for this connector's raw + state files in LOCAL mode.

    Defaults to `data/dev/` relative to cwd (override with DATA_DIR env
    var). In local mode this is where raw + state files live; connectors
    use filesystem primitives freely (Path, glob, gzip.open, etc.).

    In cloud mode raw + state do NOT live here: `raw_uri()` / `state_uri()`
    return `s3://` URIs directly and io.py streams reads/writes straight to
    R2 via fsspec/s3fs — there is no hydrate/flush bookend. `get_data_dir()`
    is then only an ephemeral local scratch directory inside the GitHub
    Actions checkout (the runner ensures it exists for any incidental
    local writes).

    Subset Delta tables live at `s3://` in cloud regardless — deltalake
    manages its own storage layer (see `subsets_uri`).
    """
    return os.environ.get('DATA_DIR', 'data/dev')


# =============================================================================
# Environment Validation
# =============================================================================

def validate_environment(additional_required: list[str] = None):
    """Validate required environment variables based on execution mode.

    Local mode: requires nothing (DATA_DIR defaults to "data").
    Cloud mode: requires R2 credentials.
    """
    if is_cloud():
        required = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"]
    else:
        required = []

    if additional_required:
        required.extend(additional_required)

    missing = [var for var in required if var not in os.environ]
    if missing:
        mode = "cloud" if is_cloud() else "local"
        raise ValueError(f"Missing required environment variables for {mode} mode: {missing}")


# =============================================================================
# R2/S3 Storage Options (DeltaLake)
# =============================================================================

def get_storage_options() -> dict | None:
    """Get storage options for DeltaLake S3 writes. Returns None for local mode.

    Thin shim over StorageBackend.deltalake_options() — kept for callers that
    don't have a URI in hand.
    """
    from .storage import backend
    return backend.deltalake_options()


def get_bucket_name() -> str:
    """Get R2 bucket name."""
    return os.environ['R2_BUCKET_NAME']


# =============================================================================
# fsspec backend — unified I/O over local file: and R2 s3://
#
# All raw + state I/O in io.py dispatches through get_fs(uri). For local
# paths this returns the local filesystem; for s3:// URIs it returns an
# s3fs filesystem pointed at R2. Connectors never see the difference —
# they call save_raw_*/load_raw_*/raw_writer, which route through here.
#
# In cloud, raw_uri() / state_uri() return s3:// URIs directly, so io.py
# streams writes straight to R2 via s3fs multipart upload — there is no
# hydrate/flush bookend. In local mode they return local paths.
# =============================================================================

def get_fsspec_storage_options(uri: str) -> dict:
    """fsspec storage_options for a URI. Empty for local, R2 creds for s3://.

    Thin shim over StorageBackend.fsspec_storage_options().
    """
    from .storage import backend
    return backend.fsspec_storage_options(uri)


def get_fs(uri: str = ""):
    """fsspec filesystem for a URI. Protocol-dispatched, cached by fsspec.

    Thin shim over StorageBackend.fsspec_fs(); see there for the R2
    fixed_upload_size quirk.
    """
    from .storage import backend
    return backend.fsspec_fs(uri)


# =============================================================================
# Path / URI Builders
#
# All save/load functions in io.py call these to get a uri (s3:// in cloud,
# local path otherwise). Dispatch on uri prefix is in io.py's _read_bytes /
# _write_bytes helpers.
# =============================================================================

def get_r2_prefix() -> str:
    """Optional path prefix under the bucket (from `R2_PREFIX`, empty default).

    Lets this project share an R2 bucket with another without slug collisions:
    with `R2_PREFIX=harness`, a connector's data lives under
    `<bucket>/harness/<connector>/...` instead of `<bucket>/<connector>/...`.
    """
    return os.environ.get("R2_PREFIX", "").strip("/")


def get_r2_base() -> str:
    """Get R2 base path for current connector: [<prefix>/]<connector>/data

    Durable, run-independent data lives here: state files (cursors that must
    survive across runs) and subset Delta tables (Delta is itself the versioned
    artifact). Raw does NOT — see get_r2_run_base().
    """
    prefix = get_r2_prefix()
    base = f"{get_connector_name()}/data"
    return f"{prefix}/{base}" if prefix else base


def get_r2_run_base() -> str:
    """Get R2 base for THIS run's artifacts: [<prefix>/]<connector>/runs/<run_id>

    Raw assets are colocated here (next to run.json/logs/memory) so each run is
    a self-contained, versioned snapshot — runs no longer overwrite each other
    in place. Mirrors runtime.run_dir(slug, run_id) on the reader side so the
    keys raw_uri() writes string-match what bucket_state() lists.

    RUN_ID is set by the runner for the whole execution; a missing one in cloud
    is a programming error (raw would otherwise silently land at runs/unknown).
    """
    run_id = os.environ.get("RUN_ID")
    if not run_id:
        raise RuntimeError(
            "RUN_ID not set — cannot build a run-scoped raw URI in cloud mode"
        )
    prefix = get_r2_prefix()
    base = f"{get_connector_name()}/runs/{run_id}"
    return f"{prefix}/{base}" if prefix else base


def raw_uri(asset_id: str, ext: str = "parquet", *, entity_id: str | None = None) -> str:
    """URI for a raw asset. s3:// in cloud, local path in dev.

    In cloud, raw is run-scoped: <connector>/runs/<run_id>/raw/... (see
    get_r2_run_base) so each run keeps its own versioned snapshot. Reads and
    writes both route through here, so within a single execution (one RUN_ID)
    they resolve to the same run automatically — no manifest needed.

    When entity_id is given, namespaces the path under <entity_id>/. This is
    the meta entity-prefix layout; legacy callers (data-integrations
    connectors) pass entity_id=None and get the flat path.

    Local mode stays flat (data/dev/raw/...) — see raw_path."""
    if is_cloud():
        sub = f"{entity_id}/{asset_id}" if entity_id is not None else asset_id
        return f"s3://{get_bucket_name()}/{get_r2_run_base()}/raw/{sub}.{ext}"
    return raw_path(asset_id, ext, entity_id=entity_id)


def state_uri(asset: str) -> str:
    """URI for a state file. s3:// in cloud, local path in dev.

    State writes are direct PUTs in cloud — each `save_state()` call
    results in one R2 PUT operation. Typical checkpointing connectors
    make hundreds of these per run; cost is negligible (~$5/month delta
    across the whole fleet — see cost analysis).
    """
    if is_cloud():
        return f"s3://{get_bucket_name()}/{get_r2_base()}/state/{asset}.json"
    return state_path(asset)


def subsets_uri(dataset_name: str) -> str:
    """URI for a subsets Delta table (s3:// in cloud, local path otherwise).

    Cloud writes live under the connector's own prefix
    (<connector>/datasets/<dataset_name>) — the Subsets server poller
    walks connector roots from the repo, not a global namespace.
    """
    if is_cloud():
        return f"s3://{get_bucket_name()}/{get_r2_base()}/subsets/{dataset_name}"
    return str(Path(get_data_dir()) / "subsets" / dataset_name)


def raw_path(asset_id: str, ext: str = "parquet", *, entity_id: str | None = None) -> str:
    """Local path for a raw asset. Creates parent dirs.

    When entity_id is given, namespaces under data/raw/<entity_id>/ — meta
    entity-prefix layout. When None (default), flat data/raw/<asset>.<ext> —
    legacy layout used by data-integrations connectors."""
    base = Path(get_data_dir()) / "raw"
    if entity_id is not None:
        path = base / entity_id / f"{asset_id}.{ext}"
    else:
        path = base / f"{asset_id}.{ext}"
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def state_path(asset: str) -> str:
    """Local path for a state file. Creates parent dirs."""
    path = Path(get_data_dir()) / "state" / f"{asset}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)
