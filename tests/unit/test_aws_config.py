"""Unit tests for AwsConfig (src/traffic_data_elt/config/settings.py).

Covers:
- from_env() required/optional variable handling
- Bronze object-key construction and slash normalisation
- configuration validation errors
"""

from __future__ import annotations

import pytest

from traffic_data_elt.config import AwsConfig


class TestAwsConfigFromEnv:
    def test_requires_region(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.setenv("S3_BUCKET", "b")
        with pytest.raises(EnvironmentError, match="AWS_REGION"):
            AwsConfig.from_env(dotenv_path=None)

    def test_requires_bucket(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "eu-central-1")
        monkeypatch.delenv("S3_BUCKET", raising=False)
        with pytest.raises(EnvironmentError, match="S3_BUCKET"):
            AwsConfig.from_env(dotenv_path=None)

    def test_defaults_applied(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "eu-central-1")
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        for var in (
            "S3_BRONZE_PREFIX",
            "S3_MULTIPART_CHUNK_BYTES",
            "S3_MULTIPART_THRESHOLD_BYTES",
            "HTTP_STREAM_CHUNK_BYTES",
        ):
            monkeypatch.delenv(var, raising=False)

        cfg = AwsConfig.from_env(dotenv_path=None)
        assert cfg.region == "eu-central-1"
        assert cfg.bucket == "my-bucket"
        assert cfg.bronze_prefix == "bronze"
        assert cfg.multipart_chunk_bytes == 8 * 1024 * 1024
        assert cfg.multipart_threshold_bytes == 8 * 1024 * 1024
        assert cfg.http_chunk_bytes == 1 * 1024 * 1024

    def test_overrides_from_env(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.setenv("S3_BUCKET", "bkt")
        monkeypatch.setenv("S3_BRONZE_PREFIX", "raw-bronze")
        monkeypatch.setenv("S3_MULTIPART_CHUNK_BYTES", "16777216")
        monkeypatch.setenv("HTTP_STREAM_CHUNK_BYTES", "524288")

        cfg = AwsConfig.from_env(dotenv_path=None)
        assert cfg.bronze_prefix == "raw-bronze"
        assert cfg.multipart_chunk_bytes == 16777216
        assert cfg.http_chunk_bytes == 524288


class TestBronzeKey:
    @pytest.fixture
    def cfg(self):
        return AwsConfig(region="eu-central-1", bucket="b", bronze_prefix="bronze")

    def test_simple_key(self, cfg):
        assert cfg.bronze_key("test", "sample.csv") == "bronze/test/sample.csv"

    def test_normalises_slashes(self, cfg):
        assert cfg.bronze_key("pneuma/2018/", "/archive.zip") == "bronze/pneuma/2018/archive.zip"

    def test_drops_empty_segments(self, cfg):
        assert cfg.bronze_key("", "test", "", "x.csv") == "bronze/test/x.csv"

    def test_custom_prefix(self):
        cfg = AwsConfig(region="r", bucket="b", bronze_prefix="lake/bronze")
        assert cfg.bronze_key("a.txt") == "lake/bronze/a.txt"

    def test_empty_raises(self, cfg):
        cfg_no_prefix = AwsConfig(region="r", bucket="b", bronze_prefix="")
        with pytest.raises(ValueError):
            cfg_no_prefix.bronze_key("")
