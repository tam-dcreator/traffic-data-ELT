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
            "Copy v1_local/.env.example to v1_local/.env and fill in real values."
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
