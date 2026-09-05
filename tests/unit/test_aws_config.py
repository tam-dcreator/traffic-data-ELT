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
            "S3_BRONZE_LAYER_PREFIX",
            "S3_SILVER_LAYER_PREFIX",
            "S3_GOLD_LAYER_PREFIX",
            "S3_BRONZE_DATA_PREFIX",
            "S3_SILVER_DATA_PREFIX",
            "S3_GOLD_DATA_PREFIX",
            "ZENODO_URL",
            "S3_MULTIPART_CHUNK_BYTES",
            "S3_MULTIPART_THRESHOLD_BYTES",
            "HTTP_STREAM_CHUNK_BYTES",
        ):
            monkeypatch.delenv(var, raising=False)

        cfg = AwsConfig.from_env(dotenv_path=None)
        assert cfg.region == "eu-central-1"
        assert cfg.bucket == "my-bucket"
        assert cfg.bronze_layer_prefix == "bronze"
        assert cfg.silver_layer_prefix == "silver"
        assert cfg.gold_layer_prefix == "gold"
        assert cfg.bronze_data_prefix == "pneuma"
        assert cfg.silver_data_prefix == "pneuma/trajectories"
        assert cfg.gold_data_prefix == "pneuma/trajectory_summary"
        assert cfg.zenodo_url == ""
        assert cfg.multipart_chunk_bytes == 8 * 1024 * 1024
        assert cfg.multipart_threshold_bytes == 8 * 1024 * 1024
        assert cfg.http_chunk_bytes == 1 * 1024 * 1024

    def test_overrides_from_env(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-east-2")
        monkeypatch.setenv("S3_BUCKET", "bkt")
        monkeypatch.setenv("S3_BRONZE_LAYER_PREFIX", "raw-bronze")
        monkeypatch.setenv("S3_BRONZE_DATA_PREFIX", "traffic")
        monkeypatch.setenv("ZENODO_URL", "https://example.org/x.zip?download=1")
        monkeypatch.setenv("S3_MULTIPART_CHUNK_BYTES", "16777216")
        monkeypatch.setenv("HTTP_STREAM_CHUNK_BYTES", "524288")

        cfg = AwsConfig.from_env(dotenv_path=None)
        assert cfg.bronze_layer_prefix == "raw-bronze"
        assert cfg.bronze_data_prefix == "traffic"
        assert cfg.zenodo_url == "https://example.org/x.zip?download=1"
        assert cfg.multipart_chunk_bytes == 16777216
        assert cfg.http_chunk_bytes == 524288


class TestBronzeKey:
    @pytest.fixture
    def cfg(self):
        # LAYER=bronze, DATA=pneuma → keys resolve under "bronze/pneuma".
        return AwsConfig(
            region="eu-central-1",
            bucket="b",
            bronze_layer_prefix="bronze",
            bronze_data_prefix="pneuma",
        )

    def test_simple_key(self, cfg):
        assert cfg.bronze_key("sample.zip") == "bronze/pneuma/sample.zip"

    def test_no_parts_returns_layer_and_data(self, cfg):
        assert cfg.bronze_key() == "bronze/pneuma"

    def test_normalises_slashes(self, cfg):
        assert cfg.bronze_key("2018/", "/archive.zip") == "bronze/pneuma/2018/archive.zip"

    def test_drops_empty_segments(self, cfg):
        assert cfg.bronze_key("", "sub", "", "x.zip") == "bronze/pneuma/sub/x.zip"

    def test_layer_data_do_not_double_slash(self):
        cfg = AwsConfig(
            region="r", bucket="b",
            bronze_layer_prefix="bronze/", bronze_data_prefix="/pneuma/",
        )
        assert cfg.bronze_key("f.zip") == "bronze/pneuma/f.zip"

    def test_empty_layer_and_data_raises(self):
        cfg = AwsConfig(
            region="r", bucket="b", bronze_layer_prefix="", bronze_data_prefix="",
        )
        with pytest.raises(ValueError):
            cfg.bronze_key("")


class TestCanonicalRoots:
    @pytest.fixture
    def cfg(self):
        return AwsConfig(region="us-east-2", bucket="traffic-data-v2-use2")

    def test_bronze_root(self, cfg):
        assert cfg.bronze_root() == "s3://traffic-data-v2-use2/bronze/pneuma"

    def test_silver_root(self, cfg):
        assert cfg.silver_root() == "s3://traffic-data-v2-use2/silver/pneuma/trajectories"

    def test_gold_root(self, cfg):
        assert cfg.gold_root() == "s3://traffic-data-v2-use2/gold/pneuma/trajectory_summary"

    def test_bronze_root_with_object(self, cfg):
        assert (
            cfg.bronze_root("pNEUMA_dataset.zip")
            == "s3://traffic-data-v2-use2/bronze/pneuma/pNEUMA_dataset.zip"
        )

    def test_no_test_segment_in_production_roots(self, cfg):
        for root in (cfg.bronze_root(), cfg.silver_root(), cfg.gold_root()):
            assert "/test/" not in root and not root.endswith("/test")
