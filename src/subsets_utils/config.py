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
    """Root directory for this connector's raw + state files.

    Defaults to `data/dev/` relative to cwd (override with DATA_DIR env
    var). Symmetric in local and cloud — in cloud, cwd is the GitHub
    Actions workspace, so this resolves to an ephemeral directory
    inside the checkout.

    Persistence: the subsets runner bookends each cloud invocation by
    hydrating `<connector>/data/{raw,state}/*` from R2 before the
    subprocess starts and flushing local changes back to R2 after it
    exits (see meta/subsets_utils/runner.py). From a connector's
    perspective, `get_data_dir()` is just "a directory with your
    persistent raw + state files" in both modes — use filesystem
    primitives freely (Path, glob, gzip.open, etc.).

    Subset Delta tables live at `s3://` in cloud regardless — deltalake
    manages its own storage layer (see `subsets_uri`).
    """
    return os.environ.get('DATA_DIR', 'data/dev')


# =============================================================================
# Dev read-fallback — read-only directory consulted when a raw/state file
# isn't in the local dev dir yet.
#
# Purely a local-dev convenience: lets dev runs reuse data already fetched
# elsewhere (e.g. an external reflection of prod state) without re-downloading.
# OPT-IN and dev-only — disabled unless SUBSETS_MIRROR_ROOT points at an
# existing directory; never consulted in cloud or for writes. The library
# carries no machine-specific default path; the location is entirely the
# operator's to supply via the env var.
# =============================================================================

def get_mirror_root() -> Path | None:
    """Root of the read-only dev fallback, or None when unset/missing.

    Set SUBSETS_MIRROR_ROOT to enable. Returns None if the var is unset or the
    path doesn't exist — callers skip the fallback in that case.
    """
    root = os.environ.get('SUBSETS_MIRROR_ROOT')
    if not root:
        return None
    p = Path(root)
    return p if p.exists() else None


def mirror_raw_path(asset_id: str, ext: str = "parquet") -> Path | None:
    """Path to a raw asset in the dev fallback. None if fallback unavailable."""
    root = get_mirror_root()
    if root is None:
        return None
    return root / get_connector_name() / "data" / "raw" / f"{asset_id}.{ext}"


def mirror_state_path(asset: str) -> Path | None:
    """Path to a state file in the dev fallback. None if fallback unavailable."""
    root = get_mirror_root()
    if root is None:
        return None
    return root / get_connector_name() / "data" / "state" / f"{asset}.json"


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
    """Get storage options for DeltaLake S3 writes. Returns None for local mode."""
    if not is_cloud():
        return None
    return {
        'AWS_ENDPOINT_URL': f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        'AWS_ACCESS_KEY_ID': os.environ['R2_ACCESS_KEY_ID'],
        'AWS_SECRET_ACCESS_KEY': os.environ['R2_SECRET_ACCESS_KEY'],
        'AWS_REGION': 'auto',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    }


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
# Today raw_uri() / state_uri() still return local paths in cloud (the
# runner bookend hydrates/flushes from R2). When the bookend is removed,
# those URIs will flip to s3:// and the same io.py code will stream
# writes directly to R2 via s3fs multipart upload — no code changes in
# io.py required.
# =============================================================================

def get_fsspec_storage_options(uri: str) -> dict:
    """fsspec storage_options for a URI. Empty for local, R2 creds for s3://."""
    if not uri.startswith("s3://"):
        return {}
    return {
        "endpoint_url": f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        "key": os.environ["R2_ACCESS_KEY_ID"],
        "secret": os.environ["R2_SECRET_ACCESS_KEY"],
        "client_kwargs": {"region_name": "auto"},
    }


def get_fs(uri: str = ""):
    """fsspec filesystem for a URI. Protocol-dispatched, cached by fsspec.

    For s3:// URIs returns s3fs pointed at R2 (requires `s3fs` installed).
    For everything else returns the local filesystem with auto_mkdir so
    parent dirs are created transparently on open.
    """
    import fsspec
    if uri.startswith("s3://"):
        # Cloudflare R2 rejects a multipart upload whose non-final parts differ
        # in size ("All non-trailing parts must have the same length"). s3fs only
        # emits uniform parts when constructed with fixed_upload_size=True, so any
        # multipart-sized write streamed straight to R2 (e.g. a large parquet from
        # raw_parquet_writer) needs it. Passed as a constructor kwarg so it lands
        # in fsspec's instance-cache key and is set correctly at build time.
        return fsspec.filesystem(
            "s3", fixed_upload_size=True, **get_fsspec_storage_options("s3://")
        )
    return fsspec.filesystem("file", auto_mkdir=True)


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
    """Get R2 base path for current connector: [<prefix>/]<connector>/data"""
    prefix = get_r2_prefix()
    base = f"{get_connector_name()}/data"
    return f"{prefix}/{base}" if prefix else base


def raw_uri(asset_id: str, ext: str = "parquet", *, entity_id: str | None = None) -> str:
    """URI for a raw asset. s3:// in cloud, local path in dev.

    When entity_id is given, namespaces the path under <entity_id>/. This is
    the meta entity-prefix layout; legacy callers (data-integrations
    connectors) pass entity_id=None and get the flat path."""
    if is_cloud():
        if entity_id is not None:
            return f"s3://{get_bucket_name()}/{get_r2_base()}/raw/{entity_id}/{asset_id}.{ext}"
        return f"s3://{get_bucket_name()}/{get_r2_base()}/raw/{asset_id}.{ext}"
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
