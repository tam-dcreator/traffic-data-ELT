"""Unit tests for NeonConfig (data-plane Neon connection config).

No live database, no secrets — pure config logic.
"""

from __future__ import annotations

import pytest

from traffic_data_elt.config import NeonConfig


# Synthetic (non-account) values — never a real Neon endpoint/host.
_SYNTH_HOST = "ep-synthetic-example-pooler.region.aws.neon.tech"


def _cfg(**over):
    base = {
        "host": _SYNTH_HOST,
        "port": 5432,
        "database": "test_db",
        "user": "test_role",
        # Sentinel, not a credential — asserted to be redacted from repr/dsn.
        "password": "SENTINEL_PLACEHOLDER_VALUE",
        "sslmode": "require",
    }
    base.update(over)
    return NeonConfig(**base)


class TestSecretHandling:
    def test_repr_redacts_password(self):
        assert "SENTINEL_PLACEHOLDER_VALUE" not in repr(_cfg())
        assert "redacted" in repr(_cfg()).lower()

    def test_dsn_excludes_password(self):
        dsn = _cfg().dsn
        assert "SENTINEL_PLACEHOLDER_VALUE" not in dsn
        assert "password" not in dsn.lower()

    def test_dsn_includes_nonsecret_fields(self):
        dsn = _cfg().dsn
        assert f"host={_SYNTH_HOST}" in dsn
        assert "dbname=test_db" in dsn
        assert "user=test_role" in dsn
        assert "sslmode=require" in dsn

    def test_conninfo_includes_password(self):
        ci = _cfg().conninfo()
        assert ci["password"] == "SENTINEL_PLACEHOLDER_VALUE"
        assert ci["dbname"] == "test_db"
        assert ci["sslmode"] == "require"
        assert ci["host"] == _SYNTH_HOST
        assert ci["port"] == 5432


class TestFromEnv:
    def test_from_env_reads_neon_db_vars(self, monkeypatch):
        monkeypatch.setenv("NEON_DB_HOST", "h.neon.tech")
        monkeypatch.setenv("NEON_DB_NAME", "db")
        monkeypatch.setenv("NEON_DB_USER", "u")
        monkeypatch.setenv("NEON_DB_PASSWORD", "p")
        # port/sslmode omitted -> defaults
        monkeypatch.delenv("NEON_DB_PORT", raising=False)
        monkeypatch.delenv("NEON_DB_SSLMODE", raising=False)
        cfg = NeonConfig.from_env(dotenv_path=None)
        assert cfg.host == "h.neon.tech"
        assert cfg.database == "db"
        assert cfg.user == "u"
        assert cfg.port == 5432          # default
        assert cfg.sslmode == "require"  # default

    def test_from_env_respects_overrides(self, monkeypatch):
        monkeypatch.setenv("NEON_DB_HOST", "h")
        monkeypatch.setenv("NEON_DB_NAME", "db")
        monkeypatch.setenv("NEON_DB_USER", "u")
        monkeypatch.setenv("NEON_DB_PASSWORD", "p")
        monkeypatch.setenv("NEON_DB_PORT", "6543")
        monkeypatch.setenv("NEON_DB_SSLMODE", "verify-full")
        cfg = NeonConfig.from_env(dotenv_path=None)
        assert cfg.port == 6543
        assert cfg.sslmode == "verify-full"

    def test_from_env_missing_required_raises(self, monkeypatch):
        for k in ("NEON_DB_HOST", "NEON_DB_NAME", "NEON_DB_USER", "NEON_DB_PASSWORD"):
            monkeypatch.delenv(k, raising=False)
        with pytest.raises(EnvironmentError):
            NeonConfig.from_env(dotenv_path=None)
