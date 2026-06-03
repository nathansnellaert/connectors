"""StorageBackend — the single object that knows how to talk to R2.

One source of truth for two concerns that used to be smeared across config.py,
io.py, duckdb.py and runner.py:

1. R2 credentials in three dialects — deltalake `storage_options`, fsspec/s3fs
   `storage_options`, and the DuckDB `SET s3_*` block. They all read the same
   R2_* env vars; here they live side by side so a credential change is a
   one-line edit, not a four-module hunt.

2. Byte-level I/O over local `file://` and R2 `s3://`, dispatched purely on the
   URI scheme via fsspec. `read_bytes` / `write_bytes` / `upload_file` /
   `exists` / `delete` are the whole vocabulary; everything else (parquet,
   ndjson, state JSON, log evacuation) is built on top.

The backend is stateless — every method reads `os.environ` at call time, just
like the free functions it replaces — so there is no lifetime/caching concern.
s3fs itself is instance-cached by fsspec on its constructor kwargs, so handing
out a fresh `fsspec_fs()` per call still reuses one underlying client.

This module is the one place in the storage plane that is allowed to consult
`is_cloud()` for the credential dialects that have no URI to dispatch on
(the deltalake no-URI path and the DuckDB SET block). URI-bearing operations
dispatch on the `s3://` scheme instead.
"""

import os
from typing import Optional

from .config import is_cloud


class StorageBackend:
    # =========================================================================
    # Credential dialects
    # =========================================================================

    def _r2_deltalake_creds(self) -> dict:
        return {
            'AWS_ENDPOINT_URL': f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            'AWS_ACCESS_KEY_ID': os.environ['R2_ACCESS_KEY_ID'],
            'AWS_SECRET_ACCESS_KEY': os.environ['R2_SECRET_ACCESS_KEY'],
            'AWS_REGION': 'auto',
            'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
        }

    def deltalake_options(self, uri: str | None = None) -> dict | None:
        """storage_options for deltalake writes. None when the target is local.

        When a `uri` is given, dispatches on its scheme (`s3://` → R2 creds,
        anything else → None). Without a uri, falls back to is_cloud() — the
        legacy no-argument behavior used by the thin config shim.
        """
        if uri is not None:
            return self._r2_deltalake_creds() if uri.startswith("s3://") else None
        if not is_cloud():
            return None
        return self._r2_deltalake_creds()

    def fsspec_storage_options(self, uri: str) -> dict:
        """fsspec storage_options for a URI. Empty for local, R2 creds for s3://."""
        if not uri.startswith("s3://"):
            return {}
        return {
            "endpoint_url": f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "key": os.environ["R2_ACCESS_KEY_ID"],
            "secret": os.environ["R2_SECRET_ACCESS_KEY"],
            "client_kwargs": {"region_name": "auto"},
        }

    def fsspec_fs(self, uri: str = ""):
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
                "s3", fixed_upload_size=True, **self.fsspec_storage_options("s3://")
            )
        return fsspec.filesystem("file", auto_mkdir=True)

    def duckdb_setup(self, con=None) -> None:
        """Configure a DuckDB connection (or the default module connection) for R2.

        No-op in local mode. In cloud, issues the `SET s3_*` block pointing
        DuckDB at the R2 endpoint with the run's credentials. Pass a specific
        connection via `con`; default (None) configures the duckdb module's
        implicit connection.
        """
        if not is_cloud():
            return
        sql = f"""
            SET s3_endpoint='{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com';
            SET s3_access_key_id='{os.environ['R2_ACCESS_KEY_ID']}';
            SET s3_secret_access_key='{os.environ['R2_SECRET_ACCESS_KEY']}';
            SET s3_region='auto';
        """
        if con is not None:
            con.execute(sql)
        else:
            import duckdb
            duckdb.sql(sql)

    # =========================================================================
    # Byte-level I/O — dispatched on URI scheme via fsspec
    # =========================================================================

    def read_bytes(self, uri: str) -> Optional[bytes]:
        """Read bytes from a URI (s3:// or local). Returns None if not found."""
        fs = self.fsspec_fs(uri)
        try:
            with fs.open(uri, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def write_bytes(self, uri: str, data: bytes) -> None:
        """Write bytes to a URI (s3:// or local) via fsspec."""
        fs = self.fsspec_fs(uri)
        with fs.open(uri, "wb") as f:
            f.write(data)

    def upload_file(self, path: str, uri: str) -> None:
        """Upload a local file to a URI (s3:// or local) via fsspec put_file."""
        fs = self.fsspec_fs(uri)
        fs.put_file(path, uri)

    def exists(self, uri: str) -> bool:
        """Check if a URI exists."""
        return self.fsspec_fs(uri).exists(uri)

    def delete(self, uri: str) -> None:
        """Delete a URI. No-op if already absent."""
        fs = self.fsspec_fs(uri)
        if fs.exists(uri):
            fs.rm(uri)


# Module singleton. The backend is stateless, so a shared instance is purely
# an ergonomic convenience — there is no per-instance state to coordinate.
backend = StorageBackend()
