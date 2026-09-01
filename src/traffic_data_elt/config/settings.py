"""Runtime configuration loaded from environment variables.

All secrets and connection details are read from the environment.
No defaults for credentials — missing required values raise at import time
so misconfiguration is visible immediately rather than failing silently
at runtime.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _require(name: str) -> str:
    """Return the value of an environment variable or raise clearly."""
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            "Copy .env.example to .env and fill in real values."
        )
    return value


def _optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class WarehouseConfig:
    """Connection details for the traffic_dwh database."""

    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        """psycopg-compatible connection string (no password in logs)."""
        return (
            f"host={self.host} port={self.port} "
            f"dbname={self.database} user={self.user}"
        )

    @property
    def url(self) -> str:
        """SQLAlchemy-style URL. Use only where required — prefer dsn."""
        return (
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class IngestionConfig:
    """File-level ingestion settings."""

    # Directory where pNEUMA sample CSV files are placed.
    data_dir: str = field(default_factory=lambda: _optional("TRAFFIC_DATA_DIR", "/data/sample"))
    # Maximum rows to process per file; 0 means no limit.
    row_limit: int = field(
        default_factory=lambda: int(_optional("TRAFFIC_INGEST_ROW_LIMIT", "0"))
    )


# Default multipart tuning for boto3 managed transfers.
# 8 MiB chunks / 8 MiB threshold keeps memory bounded during streaming
# uploads of large objects (matches boto3 defaults; exposed for tuning).
_DEFAULT_MULTIPART_CHUNK_BYTES = 8 * 1024 * 1024
_DEFAULT_MULTIPART_THRESHOLD_BYTES = 8 * 1024 * 1024
# Default HTTP streaming chunk size for the remote extractor.
_DEFAULT_HTTP_CHUNK_BYTES = 1 * 1024 * 1024


@dataclass(frozen=True)
class AwsConfig:
    """AWS / S3 settings for the V2 cloud data lake.

    AWS *credentials* are intentionally not stored here.  boto3 resolves
    credentials through its standard provider chain (environment variables,
    shared config/credentials files, or an attached IAM role), so this project
    never handles raw keys directly.

    Only non-secret configuration lives here: region, target bucket, the
    Bronze key prefix, and transfer tuning parameters.
    """

    region: str
    bucket: str
    bronze_prefix: str = "bronze"
    multipart_chunk_bytes: int = _DEFAULT_MULTIPART_CHUNK_BYTES
    multipart_threshold_bytes: int = _DEFAULT_MULTIPART_THRESHOLD_BYTES
    http_chunk_bytes: int = _DEFAULT_HTTP_CHUNK_BYTES

    @classmethod
    def from_env(cls, dotenv_path: str = "v2_cloud/.env") -> "AwsConfig":
        """Build AwsConfig from the current process environment.

        Attempts to load *dotenv_path* before reading env vars so that
        ``v2_cloud/.env`` is picked up automatically without requiring the
        caller to pre-export variables.  Shell-exported values take precedence
        (``override=False``), so CI and container environments are unaffected.
        ``python-dotenv`` is optional; if it is not installed the method falls
        back silently to the ambient environment.

        Required:
            AWS_REGION            e.g. eu-central-1
            S3_BUCKET             existing bucket name (never created here)

        Optional:
            S3_BRONZE_PREFIX               (default: bronze)
            S3_MULTIPART_CHUNK_BYTES       (default: 8 MiB)
            S3_MULTIPART_THRESHOLD_BYTES   (default: 8 MiB)
            HTTP_STREAM_CHUNK_BYTES        (default: 1 MiB)
        """
        try:
            from dotenv import load_dotenv

            load_dotenv(dotenv_path, override=False)
        except ImportError:
            pass  # python-dotenv not installed; rely on ambient environment

        return cls(
            region=_require("AWS_REGION"),
            bucket=_require("S3_BUCKET"),
            bronze_prefix=_optional("S3_BRONZE_PREFIX", "bronze"),
            multipart_chunk_bytes=int(
                _optional(
                    "S3_MULTIPART_CHUNK_BYTES", str(_DEFAULT_MULTIPART_CHUNK_BYTES)
                )
            ),
            multipart_threshold_bytes=int(
                _optional(
                    "S3_MULTIPART_THRESHOLD_BYTES",
                    str(_DEFAULT_MULTIPART_THRESHOLD_BYTES),
                )
            ),
            http_chunk_bytes=int(
                _optional("HTTP_STREAM_CHUNK_BYTES", str(_DEFAULT_HTTP_CHUNK_BYTES))
            ),
        )

    def bronze_key(self, *parts: str) -> str:
        """Construct an S3 object key under the Bronze prefix.

        Joins the configured ``bronze_prefix`` with the supplied path parts,
        normalising slashes so the result never contains empty or doubled
        segments.

        Example
        -------
        ``AwsConfig(..., bronze_prefix="bronze").bronze_key("test", "sample.csv")``
        returns ``"bronze/test/sample.csv"``.
        """
        segments: list[str] = []
        for raw in (self.bronze_prefix, *parts):
            if raw is None:
                continue
            # Split on '/' to flatten pre-joined parts and drop empties.
            for seg in str(raw).strip("/").split("/"):
                if seg:
                    segments.append(seg)
        if not segments:
            raise ValueError("bronze_key requires at least one non-empty segment")
        return "/".join(segments)


@dataclass(frozen=True)
class Settings:
    """Top-level settings object.  Instantiate once per process."""

    warehouse: WarehouseConfig
    ingestion: IngestionConfig

    @classmethod
    def from_env(cls) -> "Settings":
        """Build Settings from the current process environment."""
        warehouse = WarehouseConfig(
            host=_optional("TRAFFIC_DB_HOST", "postgres"),
            port=int(_optional("TRAFFIC_DB_PORT", "5432")),
            database=_require("TRAFFIC_DB_NAME"),
            user=_require("TRAFFIC_DB_USER"),
            password=_require("TRAFFIC_DB_PASSWORD"),
        )
        ingestion = IngestionConfig()
        return cls(warehouse=warehouse, ingestion=ingestion)
