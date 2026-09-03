"""Unit tests for v2_cloud/databricks/gold_validator.py.

Tests the pure-Python branching logic of validate_gold using a lightweight
PySpark stub (no cluster / PySpark install required), mirroring the strategy in
test_silver_validator.py.

The DataFrame mock returns configurable counts for each df.filter(...).count()
and df.select(...).distinct().count() call so we exercise each check branch.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Inject pyspark stubs before importing the validator
# ---------------------------------------------------------------------------

def _inject_pyspark_stubs() -> None:
    if "pyspark" in sys.modules:
        return

    class _FakeColumn:
        def __lt__(self, other):  return _FakeColumn()
        def __gt__(self, other):  return _FakeColumn()
        def __le__(self, other):  return _FakeColumn()
        def __ge__(self, other):  return _FakeColumn()
        def __or__(self, other):  return _FakeColumn()
        def __and__(self, other): return _FakeColumn()
        def __invert__(self):     return _FakeColumn()
        def __eq__(self, other):  return _FakeColumn()  # type: ignore[override]
        def isNull(self):         return _FakeColumn()
        def alias(self, *a):      return _FakeColumn()
        def __repr__(self):       return "<FakeColumn>"

    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql_functions = MagicMock(name="pyspark.sql.functions")
    pyspark_sql_functions.col = MagicMock(side_effect=lambda name: _FakeColumn())
    # F.sum / F.abs left as auto-generated MagicMocks (support .alias()).

    pyspark_sql_types = types.ModuleType("pyspark.sql.types")
    # Superset of types used by both the Silver and Gold schema stubs so this
    # stub is safe regardless of which validator test injects it first.
    for cls_name in (
        "StructType", "StructField", "StringType", "IntegerType",
        "LongType", "DoubleType", "TimestampType",
    ):
        setattr(pyspark_sql_types, cls_name, MagicMock(name=cls_name))

    pyspark.sql = pyspark_sql  # type: ignore[attr-defined]
    pyspark_sql.functions = pyspark_sql_functions  # type: ignore[attr-defined]
    pyspark_sql.types = pyspark_sql_types  # type: ignore[attr-defined]

    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_sql_functions
    sys.modules["pyspark.sql.types"] = pyspark_sql_types


_inject_pyspark_stubs()

from v2_cloud.databricks.gold_validator import (  # noqa: E402
    GoldValidationResult,
    validate_gold,
)


# ---------------------------------------------------------------------------
# Spark mock
# ---------------------------------------------------------------------------

def _make_spark_mock(
    row_count: int = 922,
    distinct_keys: int | None = None,
    null_counts: dict | None = None,
    bad_frame_count: int = 0,
    frame_sum: int = 1_446_887,
    bad_time: int = 0,
    bad_duration: int = 0,
    bad_distance: int = 0,
    bad_speed: int = 0,
    empty_source: int = 0,
):
    from v2_cloud.databricks.schemas.gold_schema import _FIELD_DEFS

    class _FakeField:
        def __init__(self, name, type_name, nullable):
            self.name = name
            self.dataType = type(f"_{type_name}", (), {})()
            self.nullable = nullable

    fake_fields = [_FakeField(n, t, nul) for n, t, nul in _FIELD_DEFS]
    fake_schema = MagicMock()
    fake_schema.fields = fake_fields

    if distinct_keys is None:
        distinct_keys = row_count

    df = MagicMock()
    df.schema = fake_schema
    df.count.return_value = row_count

    # df.filter(...).count() order:
    #   19 null checks, then frame_count<1, start>end, duration<0,
    #   distance<0, speed<0, source empty.
    null_counts = null_counts or {}
    filter_counts: list[int] = [null_counts.get(n, 0) for n, _, _ in _FIELD_DEFS]
    filter_counts += [
        bad_frame_count,  # frame_count < 1
        bad_time,         # start_time_s > end_time_s
        bad_duration,     # duration_s < 0
        bad_distance,     # traveled_d_m < 0
        bad_speed,        # min/max speed < 0
        empty_source,     # source_file null/empty
    ]
    filter_result = MagicMock()
    filter_result.count = MagicMock(side_effect=filter_counts)
    df.filter.return_value = filter_result

    # df.select(*keys).distinct().count() for uniqueness check.
    select_result = MagicMock()
    distinct_result = MagicMock()
    distinct_result.count.return_value = distinct_keys
    select_result.distinct.return_value = distinct_result
    df.select.return_value = select_result

    # df.agg(F.sum(...)).collect()[0]["s"] for frame conservation.
    agg_row = {"s": frame_sum}
    agg_result = MagicMock()
    agg_result.collect.return_value = [agg_row]
    df.agg.return_value = agg_result

    spark = MagicMock()
    spark.read.parquet.return_value = df
    return spark


class TestGoldValidationResult:
    def test_passed_when_no_failures(self):
        r = GoldValidationResult(gold_path="s3://b/g", row_count=922)
        assert r.passed is True

    def test_failed_with_failures(self):
        r = GoldValidationResult(gold_path="s3://b/g", row_count=0)
        r.failed_checks.append("boom")
        assert r.passed is False

    def test_summary_contains_path_and_count(self):
        r = GoldValidationResult(gold_path="s3://bucket/gold/test", row_count=922)
        s = r.summary()
        assert "s3://bucket/gold/test" in s
        assert "922" in s


class TestValidateGoldPassing:
    def test_all_checks_pass(self):
        spark = _make_spark_mock(row_count=922, frame_sum=1_446_887)
        result = validate_gold(
            spark, "s3://b/gold/test/",
            expected_trajectory_count=922,
            expected_frame_sum=1_446_887,
        )
        assert result.passed, result.summary()

    def test_no_expected_values_skips_optional_checks(self):
        spark = _make_spark_mock(row_count=500, distinct_keys=500, frame_sum=999)
        result = validate_gold(spark, "s3://b/gold/")
        # No trajectory-count or frame-conservation failure without expectations.
        assert not any("trajectory count" in c for c in result.failed_checks)
        assert not any("conservation" in c for c in result.failed_checks)


class TestValidateGoldFailing:
    def test_empty_dataset_fails(self):
        spark = _make_spark_mock(row_count=0)
        result = validate_gold(spark, "s3://b/gold/")
        assert not result.passed
        assert any("row_count == 0" in c for c in result.failed_checks)

    def test_trajectory_count_mismatch_fails(self):
        spark = _make_spark_mock(row_count=900)
        result = validate_gold(spark, "s3://b/gold/", expected_trajectory_count=922)
        assert not result.passed
        assert any("trajectory count" in c.lower() for c in result.failed_checks)

    def test_duplicate_key_fails(self):
        spark = _make_spark_mock(row_count=922, distinct_keys=920)
        result = validate_gold(spark, "s3://b/gold/")
        assert not result.passed
        assert any("unique" in c for c in result.failed_checks)

    def test_frame_conservation_mismatch_fails(self):
        spark = _make_spark_mock(row_count=922, frame_sum=1_000_000)
        result = validate_gold(
            spark, "s3://b/gold/", expected_frame_sum=1_446_887
        )
        assert not result.passed
        assert any("conservation" in c.lower() for c in result.failed_checks)

    def test_bad_frame_count_fails(self):
        spark = _make_spark_mock(row_count=922, bad_frame_count=3)
        result = validate_gold(spark, "s3://b/gold/")
        assert not result.passed
        assert any("frame_count < 1" in c for c in result.failed_checks)

    def test_bad_duration_fails(self):
        spark = _make_spark_mock(row_count=922, bad_duration=1)
        result = validate_gold(spark, "s3://b/gold/")
        assert not result.passed
        assert any("duration_s < 0" in c for c in result.failed_checks)

    def test_start_after_end_fails(self):
        spark = _make_spark_mock(row_count=922, bad_time=2)
        result = validate_gold(spark, "s3://b/gold/")
        assert not result.passed
        assert any("start_time_s > end_time_s" in c for c in result.failed_checks)

    def test_negative_distance_fails(self):
        spark = _make_spark_mock(row_count=922, bad_distance=1)
        result = validate_gold(spark, "s3://b/gold/")
        assert not result.passed
        assert any("traveled_d_m < 0" in c for c in result.failed_checks)

    def test_negative_speed_fails(self):
        spark = _make_spark_mock(row_count=922, bad_speed=1)
        result = validate_gold(spark, "s3://b/gold/")
        assert not result.passed
        assert any("speed" in c for c in result.failed_checks)

    def test_null_field_fails(self):
        spark = _make_spark_mock(row_count=922, null_counts={"vehicle_type": 4})
        result = validate_gold(spark, "s3://b/gold/")
        assert not result.passed
        assert any("null" in c.lower() for c in result.failed_checks)

    def test_read_failure_captured(self):
        spark = MagicMock()
        spark.read.parquet.side_effect = RuntimeError("path missing")
        result = validate_gold(spark, "s3://b/bad/")
        assert not result.passed
        assert any("cannot read" in c for c in result.failed_checks)
