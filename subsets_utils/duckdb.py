"""DuckDB utilities for querying raw data."""

import os
import duckdb
from .config import is_cloud, raw_uri

_configured = False


def _configure():
    """Auto-configure DuckDB for S3 if in cloud mode."""
    global _configured
    if _configured:
        return

    if is_cloud():
        duckdb.sql(f"""
            SET s3_endpoint='{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com';
            SET s3_access_key_id='{os.environ['R2_ACCESS_KEY_ID']}';
            SET s3_secret_access_key='{os.environ['R2_SECRET_ACCESS_KEY']}';
            SET s3_region='auto';
        """)
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
