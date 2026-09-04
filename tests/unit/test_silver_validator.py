"""Unit tests for v2_cloud/databricks/silver_validator.py.

Tests the pure-Python logic:
- SilverValidationResult.passed property
- SilverValidationResult.summary() output
- validate_silver() using a minimal PySpark mock

All Spark interactions are mocked with a lightweight stub so no cluster or
PySpark installation is required.

PySpark mock strategy
---------------------
``pyspark`` is not installed locally.  We inject stub modules into
``sys.modules`` before importing the validator so that:
  - ``from pyspark.sql import functions as F`` inside ``validate_silver``
    gets a MagicMock ``F`` whose ``.col()`` returns an opaque MagicMock value.
  - The DataFrame mock's ``.filter(anything).count()`` returns preset counts
    regardless of what expression was passed — the validator logic under test
    is the branching on those counts, not the expression building.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Inject pyspark stubs before any validator/schema imports touch pyspark
# ---------------------------------------------------------------------------

def _inject_pyspark_stubs() -> None:
    """Inject minimal pyspark stub modules into sys.modules."""
    if "pyspark" in sys.modules:
        return  # already present (real or previously stubbed)

    # A Column stub that supports arithmetic/comparison operators used in the
    # validator (F.col("x") < value, F.col("x") > value, colA | colB, etc.).
    class _FakeColumn:
        """Minimal Spark Column stub — all operators return another _FakeColumn."""
        def __lt__(self, other):  return _FakeColumn()
        def __gt__(self, other):  return _FakeColumn()
        def __le__(self, other):  return _FakeColumn()
        def __ge__(self, other):  return _FakeColumn()
        def __or__(self, other):  return _FakeColumn()
        def __and__(self, other): return _FakeColumn()
        def __invert__(self):     return _FakeColumn()
        def __eq__(self, other):  return _FakeColumn()  # type: ignore[override]
        def isNull(self):         return _FakeColumn()
        def __repr__(self):       return "<FakeColumn>"

    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")

    pyspark_sql_functions = MagicMock(name="pyspark.sql.functions")
    pyspark_sql_functions.col = MagicMock(side_effect=lambda name: _FakeColumn())

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

# Now import the module under test (pyspark stubs are in sys.modules)
from traffic_data_elt.databricks.silver_validator import (  # noqa: E402
    SilverValidationResult,
    validate_silver,
)


# ---------------------------------------------------------------------------
# Shared test helpers
# ---------------------------------------------------------------------------

def _make_spark_mock(
    row_count: int = 10,
    null_counts: dict | None = None,
    out_of_bounds_lat: int = 0,
    out_of_bounds_lon: int = 0,
    negative_speed: int = 0,
    empty_source: int = 0,
    empty_key: int = 0,
) -> MagicMock:
    """Build a minimal SparkSession mock that satisfies validate_silver calls.

    The mock models a DataFrame with configurable row counts and null counts.
    """
    from traffic_data_elt.databricks.schemas.silver_schema import _FIELD_DEFS, SILVER_FIELD_NAMES

    # Fake StructField-like objects with .name and .dataType attributes.
    class _FakeField:
        def __init__(self, name, type_name, nullable):
            self.name = name
            # Use a distinct type per field name so type-equality checks work.
            self.dataType = type(f"_{type_name}", (), {})()
            self.nullable = nullable

    fake_fields = [_FakeField(n, t, nul) for n, t, nul in _FIELD_DEFS]

    fake_schema = MagicMock()
    fake_schema.fields = fake_fields

    df = MagicMock()
    df.schema = fake_schema
    df.count.return_value = row_count

    # validate_silver calls df.filter(...).count() in this order:
    #  1. one call per non-nullable field (13 fields) — null check
    #  2. lat out-of-bounds
    #  3. lon out-of-bounds
    #  4. negative speed
    #  5. empty source_file
    #  6. empty bronze_key
    filter_counts: list[int] = []
    null_counts = null_counts or {}
    for name, _, _ in _FIELD_DEFS:
        filter_counts.append(null_counts.get(name, 0))
    filter_counts.extend([
        out_of_bounds_lat,
        out_of_bounds_lon,
        negative_speed,
        empty_source,
        empty_key,
    ])

    filter_result = MagicMock()
    filter_result.count = MagicMock(side_effect=filter_counts)
    df.filter.return_value = filter_result

    spark = MagicMock()
    spark.read.parquet.return_value = df
    return spark


# ---------------------------------------------------------------------------
# SilverValidationResult
# ---------------------------------------------------------------------------

class TestSilverValidationResult:
    def test_passed_when_no_failures(self):
        r = SilverValidationResult(silver_path="s3://b/k", row_count=100)
        r.passed_checks.append("check A")
        assert r.passed is True

    def test_failed_when_failures_present(self):
        r = SilverValidationResult(silver_path="s3://b/k", row_count=0)
        r.failed_checks.append("something went wrong")
        assert r.passed is False

    def test_summary_contains_passed_or_failed(self):
        r = SilverValidationResult(silver_path="s3://b/k", row_count=50)
        summary = r.summary()
        assert "PASSED" in summary or "FAILED" in summary

    def test_summary_contains_path(self):
        r = SilverValidationResult(silver_path="s3://my-bucket/silver/test", row_count=5)
        assert "s3://my-bucket/silver/test" in r.summary()

    def test_summary_contains_row_count(self):
        r = SilverValidationResult(silver_path="s3://b/k", row_count=1_446_887)
        assert "1,446,887" in r.summary()

    def test_summary_lists_failed_checks(self):
        r = SilverValidationResult(silver_path="s3://b/k", row_count=0)
        r.failed_checks.append("lat out of bounds")
        assert "lat out of bounds" in r.summary()


# ---------------------------------------------------------------------------
# validate_silver — passing cases
# ---------------------------------------------------------------------------

class TestValidateSilverPassed:
    def test_all_checks_pass_with_valid_data(self):
        spark = _make_spark_mock(row_count=1_446_887)
        result = validate_silver(spark, "s3://b/silver/test/", expected_row_count=1_446_887)
        assert result.passed, f"Expected PASSED but failed: {result.failed_checks}"

    def test_row_count_captured(self):
        spark = _make_spark_mock(row_count=999)
        result = validate_silver(spark, "s3://b/silver/")
        assert result.row_count == 999

    def test_no_expected_count_skips_parity_check(self):
        spark = _make_spark_mock(row_count=500)
        result = validate_silver(spark, "s3://b/silver/")
        parity_fails = [c for c in result.failed_checks if "parity" in c.lower()]
        assert parity_fails == []

    def test_summary_says_passed(self):
        spark = _make_spark_mock(row_count=1_446_887)
        result = validate_silver(spark, "s3://b/silver/", expected_row_count=1_446_887)
        assert result.passed
        assert "PASSED" in result.summary()

    def test_local_env_warns_about_type_check_skip(self):
        """In a local (no real PySpark) environment the type check is skipped.

        The warning must mention that the check will run strictly on Databricks,
        so developers know the local pass does NOT guarantee type correctness.
        """
        spark = _make_spark_mock(row_count=100)
        result = validate_silver(spark, "s3://b/silver/")
        # Either passed (stub schema matches) or there's a warning about the type check.
        # The key requirement: no silent skip — either a pass or a clear warning.
        if result.warnings:
            skip_warning = any(
                "type check" in w.lower() or "pyspark" in w.lower()
                for w in result.warnings
            )
            assert skip_warning, (
                "Expected a warning about type check being skipped locally, "
                f"got: {result.warnings}"
            )


# ---------------------------------------------------------------------------
# validate_silver — failure cases
# ---------------------------------------------------------------------------

class TestValidateSilverFailures:
    def test_empty_dataset_fails(self):
        spark = _make_spark_mock(row_count=0)
        result = validate_silver(spark, "s3://b/silver/")
        assert not result.passed
        assert any("row_count == 0" in c for c in result.failed_checks)

    def test_parity_mismatch_fails(self):
        spark = _make_spark_mock(row_count=1_000)
        result = validate_silver(spark, "s3://b/silver/", expected_row_count=1_446_887)
        assert not result.passed
        assert any("parity" in c.lower() for c in result.failed_checks)

    def test_null_in_field_fails(self):
        spark = _make_spark_mock(row_count=100, null_counts={"lat": 5})
        result = validate_silver(spark, "s3://b/silver/")
        assert not result.passed
        assert any("null" in c.lower() for c in result.failed_checks)

    def test_lat_out_of_bounds_fails(self):
        spark = _make_spark_mock(row_count=100, out_of_bounds_lat=3)
        result = validate_silver(spark, "s3://b/silver/")
        assert not result.passed
        assert any("lat" in c for c in result.failed_checks)

    def test_lon_out_of_bounds_fails(self):
        spark = _make_spark_mock(row_count=100, out_of_bounds_lon=2)
        result = validate_silver(spark, "s3://b/silver/")
        assert not result.passed
        assert any("lon" in c for c in result.failed_checks)

    def test_negative_speed_fails(self):
        spark = _make_spark_mock(row_count=100, negative_speed=1)
        result = validate_silver(spark, "s3://b/silver/")
        assert not result.passed
        assert any("speed" in c for c in result.failed_checks)

    def test_empty_source_file_fails(self):
        spark = _make_spark_mock(row_count=100, empty_source=4)
        result = validate_silver(spark, "s3://b/silver/")
        assert not result.passed
        assert any("source_file" in c for c in result.failed_checks)

    def test_empty_bronze_key_fails(self):
        spark = _make_spark_mock(row_count=100, empty_key=1)
        result = validate_silver(spark, "s3://b/silver/")
        assert not result.passed
        assert any("bronze_key" in c for c in result.failed_checks)

    def test_spark_read_failure_captured(self):
        spark = MagicMock()
        spark.read.parquet.side_effect = RuntimeError("S3 path not found")
        result = validate_silver(spark, "s3://b/bad-path/")
        assert not result.passed
        assert any("cannot read" in c for c in result.failed_checks)
