"""Unit tests for v2_cloud/databricks/gold_transformer.py.

Covers pure-Python logic and contracts that do NOT require PySpark:
- GoldWriteResult.frames_conserved property
- build_trajectory_summary import guard (raises without PySpark)
- AwsConfig.gold_key() path construction

The build_trajectory_summary aggregation itself and write_gold require a live
SparkSession and are exercised by the opt-in Databricks integration test.
"""

from __future__ import annotations

import datetime

import pytest

from v2_cloud.databricks.gold_transformer import GoldWriteResult

_NOW = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)


def _result(silver: int, sum_fc: int) -> GoldWriteResult:
    return GoldWriteResult(
        silver_path="s3://b/silver/test/",
        gold_path="s3://b/gold/test/",
        silver_frame_count=silver,
        gold_trajectory_count=922,
        sum_frame_count=sum_fc,
        ingested_at=_NOW,
        start_time=_NOW,
        end_time=_NOW,
        status="success",
    )


class TestFramesConserved:
    def test_conserved_when_equal(self):
        assert _result(1_446_887, 1_446_887).frames_conserved is True

    def test_not_conserved_when_different(self):
        assert _result(1_446_887, 1_446_000).frames_conserved is False


class TestBuildTrajectorySummaryGuard:
    def test_requires_pyspark(self):
        import sys

        if "pyspark" in sys.modules:
            pytest.skip("pyspark present (real or stubbed) in this environment")
        from v2_cloud.databricks.gold_transformer import build_trajectory_summary

        with pytest.raises(ModuleNotFoundError):
            build_trajectory_summary(object())


class TestGoldKey:
    """AwsConfig.gold_key() — added as part of the Gold milestone."""

    @pytest.fixture
    def cfg(self):
        from traffic_data_elt.config import AwsConfig
        return AwsConfig(region="eu-central-1", bucket="b", gold_prefix="gold")

    def test_simple_gold_key(self, cfg):
        assert cfg.gold_key("pneuma", "trajectory_summary", "test") == (
            "gold/pneuma/trajectory_summary/test"
        )

    def test_normalises_slashes(self, cfg):
        assert cfg.gold_key("pneuma/", "/test/") == "gold/pneuma/test"

    def test_custom_prefix(self):
        from traffic_data_elt.config import AwsConfig
        cfg = AwsConfig(region="r", bucket="b", gold_prefix="lake/gold")
        assert cfg.gold_key("a.parquet") == "lake/gold/a.parquet"

    def test_empty_raises(self):
        from traffic_data_elt.config import AwsConfig
        cfg = AwsConfig(region="r", bucket="b", gold_prefix="")
        with pytest.raises(ValueError):
            cfg.gold_key("")

    def test_default_gold_prefix_is_gold(self):
        from traffic_data_elt.config import AwsConfig
        cfg = AwsConfig(region="r", bucket="b")
        assert cfg.gold_prefix == "gold"
