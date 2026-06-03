"""DuckDB utilities for querying raw data."""

import duckdb
from .config import raw_uri
from .storage import backend

_configured = False


def _configure():
    """Auto-configure DuckDB for S3 if in cloud mode (delegates to StorageBackend)."""
    global _configured
    if _configured:
        return
    backend.duckdb_setup()
    _configured = True


def raw(assets: list[str] | str) -> str:
    """Returns read_parquet clause for DuckDB query.

    Usage:
        from subsets_utils.duckdb import raw

        table = duckdb.sql(f"SELECT * FROM {raw('my_asset')}").arrow()
        table = duckdb.sql(f"SELECT * FROM {raw(['asset1', 'asset2'])}").arrow()
    """
    _configure()
    if isinstance(assets, str):
        assets = [assets]
    paths = [raw_uri(a, "parquet") for a in assets]
    return f"read_parquet({paths})"
