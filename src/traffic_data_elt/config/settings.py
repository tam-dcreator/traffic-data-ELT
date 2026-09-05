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
    # Medallion LAYER prefixes (top-level bucket partitions).
    bronze_layer_prefix: str = "bronze"
    silver_layer_prefix: str = "silver"
    gold_layer_prefix: str = "gold"
    # DATASET DATA prefixes (dataset path within each layer).
    bronze_data_prefix: str = "pneuma"
    silver_data_prefix: str = "pneuma/trajectories"
    gold_data_prefix: str = "pneuma/trajectory_summary"
    # Full source-archive URL (Zenodo). Empty unless configured; the Bronze
    # ingestion path requires it, but constructing AwsConfig for Silver/Gold
    # path composition does not.
    zenodo_url: str = ""
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

        Environment contract (unambiguous LAYER + DATA prefixes)
        --------------------------------------------------------
        Required:
            AWS_REGION            e.g. us-east-2
            S3_BUCKET             existing bucket name (never created here)

        Optional (medallion LAYER prefixes — top-level bucket partitions):
            S3_BRONZE_LAYER_PREFIX         (default: bronze)
            S3_SILVER_LAYER_PREFIX         (default: silver)
            S3_GOLD_LAYER_PREFIX           (default: gold)

        Optional (DATASET DATA prefixes — dataset path within each layer):
            S3_BRONZE_DATA_PREFIX          (default: pneuma)
            S3_SILVER_DATA_PREFIX          (default: pneuma/trajectories)
            S3_GOLD_DATA_PREFIX            (default: pneuma/trajectory_summary)

        Optional (ingestion + transfer tuning):
            ZENODO_URL                     (default: "")
            S3_MULTIPART_CHUNK_BYTES       (default: 8 MiB)
            S3_MULTIPART_THRESHOLD_BYTES   (default: 8 MiB)
            HTTP_STREAM_CHUNK_BYTES        (default: 1 MiB)

        This is the single, unambiguous prefix contract — the earlier flat
        ``S3_BRONZE_PREFIX`` / ``S3_SILVER_PREFIX`` / ``S3_GOLD_PREFIX`` names
        have been removed to avoid two competing conventions.
        """
        try:
            from dotenv import load_dotenv

            if dotenv_path is not None:
                load_dotenv(dotenv_path, override=False)
        except ImportError:
            print("Python-dotenv not installed,using system env variables")
            pass  # python-dotenv not installed; rely on ambient environment

        return cls(
            region=_require("AWS_REGION"),
            bucket=_require("S3_BUCKET"),
            bronze_layer_prefix=_optional("S3_BRONZE_LAYER_PREFIX", "bronze"),
            silver_layer_prefix=_optional("S3_SILVER_LAYER_PREFIX", "silver"),
            gold_layer_prefix=_optional("S3_GOLD_LAYER_PREFIX", "gold"),
            bronze_data_prefix=_optional("S3_BRONZE_DATA_PREFIX", "pneuma"),
            silver_data_prefix=_optional(
                "S3_SILVER_DATA_PREFIX", "pneuma/trajectories"
            ),
            gold_data_prefix=_optional(
                "S3_GOLD_DATA_PREFIX", "pneuma/trajectory_summary"
            ),
            zenodo_url=_optional("ZENODO_URL", ""),
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

    # ── S3 object keys (LAYER + DATA + extra parts) ───────────────────────────
    def bronze_key(self, *parts: str) -> str:
        """Construct an S3 object key under ``<bronze_layer>/<bronze_data>``.

        Example: ``bronze_key("pnemas.zip")`` → ``"bronze/pneuma/pnemas.zip"``.
        """
        return self._build_key(self.bronze_layer_prefix, self.bronze_data_prefix, *parts)

    def silver_key(self, *parts: str) -> str:
        """Construct an S3 object key under ``<silver_layer>/<silver_data>``.

        Example: ``silver_key()`` → ``"silver/pneuma/trajectories"``.
        """
        return self._build_key(self.silver_layer_prefix, self.silver_data_prefix, *parts)

    def gold_key(self, *parts: str) -> str:
        """Construct an S3 object key under ``<gold_layer>/<gold_data>``.

        Example: ``gold_key()`` → ``"gold/pneuma/trajectory_summary"``.
        """
        return self._build_key(self.gold_layer_prefix, self.gold_data_prefix, *parts)

    # ── Canonical s3:// roots (single source of path composition) ─────────────
    def s3_uri(self, key: str) -> str:
        """Return the full ``s3://<bucket>/<key>`` URI for a normalised key."""
        return f"s3://{self.bucket}/{key}"

    def bronze_root(self, *parts: str) -> str:
        """Full ``s3://`` root for Bronze data, e.g. ``s3://<bucket>/bronze/pneuma``."""
        return self.s3_uri(self.bronze_key(*parts))

    def silver_root(self, *parts: str) -> str:
        """Full ``s3://`` root for Silver data,
        e.g. ``s3://<bucket>/silver/pneuma/trajectories``."""
        return self.s3_uri(self.silver_key(*parts))

    def gold_root(self, *parts: str) -> str:
        """Full ``s3://`` root for Gold data,
        e.g. ``s3://<bucket>/gold/pneuma/trajectory_summary``."""
        return self.s3_uri(self.gold_key(*parts))

    def _build_key(self, *parts: str) -> str:
        """Construct an S3 key from ordered path parts.

        Splits every part on ``/`` to flatten pre-joined segments, drops empty
        segments, and joins with a single ``/`` — so inputs like ``"bronze/"``,
        ``"/pneuma/"``, ``""`` can never produce ``bronze//pneuma``. This is the
        one canonical slash-normalising composer; DAGs, notebooks, and modules
        must not rebuild path strings independently.
        """
        segments: list[str] = []
        for raw in parts:
            if raw is None:
                continue
            for seg in str(raw).strip("/").split("/"):
                if seg:
                    segments.append(seg)
        if not segments:
            raise ValueError("key requires at least one non-empty segment")
        return "/".join(segments)


@dataclass(frozen=True)
class NeonConfig:
    """Connection details for the Neon serving PostgreSQL database (V2).

    This is the **data-plane** connection (psycopg / dbt / the Databricks
    loader).  It is deliberately separate from ``NEON_API_KEY`` — the Neon
    control-plane API key used by the ``neon`` CLI for project/branch/endpoint
    operations.  The API key must never be used as a PostgreSQL password.

    Only non-secret fields are exposed via :pyattr:`dsn`; the password is never
    included in string representations, DSNs, or logs.  Retrieve values with
    :meth:`conninfo` (a kwargs dict for ``psycopg.connect``) when a connection
    is actually needed.
    """

    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "require"

    @classmethod
    def from_env(cls, dotenv_path: str = "v2_cloud/.env") -> "NeonConfig":
        """Build NeonConfig from the environment (loads ``v2_cloud/.env``).

        Required:
            NEON_DB_HOST
            NEON_DB_NAME
            NEON_DB_USER
            NEON_DB_PASSWORD

        Optional (with defaults):
            NEON_DB_PORT      (default: 5432)
            NEON_DB_SSLMODE   (default: require)

        Shell-exported values take precedence over the dotenv file
        (``override=False``), matching :meth:`AwsConfig.from_env`.
        """
        try:
            from dotenv import load_dotenv

            if dotenv_path is not None:
                load_dotenv(dotenv_path, override=False)
        except ImportError:
            print("Python-dotenv not installed,using system env variables")
            pass  # python-dotenv not installed; rely on ambient environment

        return cls(
            host=_require("NEON_DB_HOST"),
            port=int(_optional("NEON_DB_PORT", "5432")),
            database=_require("NEON_DB_NAME"),
            user=_require("NEON_DB_USER"),
            password=_require("NEON_DB_PASSWORD"),
            sslmode=_optional("NEON_DB_SSLMODE", "require"),
        )

    @property
    def dsn(self) -> str:
        """psycopg-style DSN **without** the password (safe to log)."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} sslmode={self.sslmode}"
        )

    def conninfo(self) -> dict:
        """Return kwargs for ``psycopg.connect`` (includes the password).

        The returned dict contains the secret — never log or print it.
        """
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
        }

    def __repr__(self) -> str:  # never leak the password in reprs / tracebacks
        return (
            f"NeonConfig(host={self.host!r}, port={self.port!r}, "
            f"database={self.database!r}, user={self.user!r}, "
            f"sslmode={self.sslmode!r}, password=<redacted>)"
        )


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
