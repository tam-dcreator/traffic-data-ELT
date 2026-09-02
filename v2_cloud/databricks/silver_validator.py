"""Silver output validator for V2 Databricks processing.

Validates the written Silver Parquet dataset before:
- Signalling pipeline success.
- Authorising temporary file cleanup.

Checks
------
1. Output path exists and contains Parquet files.
2. Row count > 0.
3. Schema matches the expected Silver contract exactly (field names and types).
4. Required non-nullable fields contain no nulls.
5. Latitude is within the Athens study area bounding box [37.9, 38.1].
6. Longitude is within the Athens study area bounding box [23.6, 23.9].
7. Speed (``speed_ms``) is non-negative.
8. ``source_file`` is populated (non-empty string) on all rows.
9. ``bronze_key`` is populated on all rows.
10. Parquet can be read back successfully.
11. (Optional) V1 parity check: frame row count matches the expected value.

Usage
-----
Obtain a ``SilverValidationResult`` via :func:`validate_silver`.
Check ``.passed`` before calling :meth:`~bronze_reader.BronzeArchive.cleanup`.

The validator raises ``SilverValidationError`` only for fatal configuration
problems.  Individual check failures are captured in ``failed_checks`` and
reported via ``.passed``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from traffic_data_elt.utils import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger(__name__)


class SilverValidationError(RuntimeError):
    """Raised for fatal configuration or framework errors during validation."""


@dataclass
class SilverValidationResult:
    """Outcome of all Silver validation checks."""

    silver_path: str
    row_count: int
    passed_checks: list[str] = field(default_factory=list)
    failed_checks: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """True when no checks failed."""
        return len(self.failed_checks) == 0

    def summary(self) -> str:
        lines = [
            f"Silver validation: {'PASSED' if self.passed else 'FAILED'}",
            f"  path:         {self.silver_path}",
            f"  row_count:    {self.row_count:,}",
            f"  passed:       {len(self.passed_checks)}",
            f"  failed:       {len(self.failed_checks)}",
        ]
        for msg in self.failed_checks:
            lines.append(f"  FAIL  {msg}")
        for msg in self.warnings:
            lines.append(f"  WARN  {msg}")
        return "\n".join(lines)


def validate_silver(
    spark: "SparkSession",
    silver_path: str,
    *,
    expected_row_count: int | None = None,
) -> SilverValidationResult:
    """Run all Silver validation checks and return a result object.

    Parameters
    ----------
    spark:
        Active SparkSession.
    silver_path:
        Full S3 path to the Silver Parquet output directory.
    expected_row_count:
        When supplied, the actual row count is compared against this value for
        V1/V2 parity verification.  A mismatch is a **failure**, not a warning,
        because parity is a milestone requirement.

    Returns
    -------
    SilverValidationResult
        Contains all check outcomes.  Check ``.passed`` before cleanup.

    Raises
    ------
    SilverValidationError
        Only for fatal framework errors (e.g. Spark unavailable).
    """
    from v2_cloud.databricks.schemas.silver_schema import (
        LAT_MAX,
        LAT_MIN,
        LON_MAX,
        LON_MIN,
        SILVER_FIELD_NAMES,
        SILVER_NON_NULLABLE_FIELDS,
        SPEED_MIN,
        get_silver_schema,
    )

    result = SilverValidationResult(silver_path=silver_path, row_count=0)

    # ── 1. Read back Silver Parquet ─────────────────────────────────────────
    try:
        df = spark.read.parquet(silver_path)
    except Exception as exc:  # noqa: BLE001
        result.failed_checks.append(
            f"cannot read Silver Parquet at {silver_path}: {exc}"
        )
        log.error("Silver read-back failed: %s", exc)
        return result  # cannot proceed with further checks

    result.passed_checks.append("Silver Parquet is readable")

    # ── 2. Row count > 0 ────────────────────────────────────────────────────
    row_count = df.count()
    result.row_count = row_count

    if row_count > 0:
        result.passed_checks.append(f"row_count > 0 (actual: {row_count:,})")
    else:
        result.failed_checks.append("row_count == 0 — Silver dataset is empty")
        return result  # no point running further checks on an empty dataset

    # ── 3. V1/V2 parity check ───────────────────────────────────────────────
    if expected_row_count is not None:
        if row_count == expected_row_count:
            result.passed_checks.append(
                f"V1/V2 parity: row_count matches expected {expected_row_count:,}"
            )
        else:
            result.failed_checks.append(
                f"V1/V2 parity FAILED: expected {expected_row_count:,} rows, "
                f"got {row_count:,} (delta: {row_count - expected_row_count:+,})"
            )
            log.error(
                "PARITY FAILURE: expected=%d actual=%d delta=%d",
                expected_row_count,
                row_count,
                row_count - expected_row_count,
            )

    # ── 4. Schema check ─────────────────────────────────────────────────────
    actual_field_names = [f.name for f in df.schema.fields]
    if actual_field_names == SILVER_FIELD_NAMES:
        result.passed_checks.append("schema field names match Silver contract")
    else:
        missing = set(SILVER_FIELD_NAMES) - set(actual_field_names)
        extra = set(actual_field_names) - set(SILVER_FIELD_NAMES)
        result.failed_checks.append(
            f"schema mismatch — missing: {sorted(missing)}, extra: {sorted(extra)}"
        )

    # Type check per field.
    # - On real Databricks (pyspark available): any type mismatch is a FAILURE.
    # - In local unit tests (pyspark absent or stubbed): block is best-effort
    #   and skipped with a warning when the schema cannot be fully resolved.
    _pyspark_available = True
    try:
        from pyspark.sql import functions as F  # noqa: PLC0415
    except ModuleNotFoundError:
        _pyspark_available = False

    if _pyspark_available:
        try:
            silver_schema = get_silver_schema()
            schema_type_map = {f.name: f.dataType for f in silver_schema.fields}
            actual_type_map = {f.name: f.dataType for f in df.schema.fields}
            type_mismatches: list[str] = []
            for name in SILVER_FIELD_NAMES:
                if name not in actual_type_map:
                    continue  # already caught by field-name check above
                if name not in schema_type_map:
                    # Schema stub incomplete — only happens when pyspark types
                    # are stubbed out (should not occur in real Spark execution).
                    result.warnings.append(
                        f"schema_type_map missing key '{name}' — type check skipped for this field"
                    )
                    continue
                expected_type = schema_type_map[name]
                actual_type = actual_type_map[name]
                if type(expected_type) is not type(actual_type):
                    type_mismatches.append(
                        f"{name}: expected {type(expected_type).__name__}, "
                        f"got {type(actual_type).__name__}"
                    )
            if type_mismatches:
                result.failed_checks.append(f"field type mismatches: {type_mismatches}")
            else:
                result.passed_checks.append("all field types match Silver schema")
        except Exception as exc:  # noqa: BLE001
            # On real Spark this should not happen.  Capture as failure so it
            # is visible in the validation report rather than silently skipped.
            result.failed_checks.append(
                f"schema type check raised an unexpected error: {exc}"
            )
            log.error("schema type check failed unexpectedly: %s", exc)
    else:
        result.warnings.append(
            "pyspark not installed locally — field type check skipped "
            "(will run strictly on real Databricks execution)"
        )

    # ── 5. Null checks on non-nullable fields ────────────────────────────────
    if not _pyspark_available:
        result.failed_checks.append(
            "pyspark unavailable locally — cannot execute data quality checks. "
            "Run on Databricks for full validation."
        )
        log.error("pyspark unavailable: data checks skipped")
        return result

    null_violations: list[str] = []
    non_nullable = SILVER_NON_NULLABLE_FIELDS
    for col_name in non_nullable:
        if col_name not in actual_field_names:
            continue
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            null_violations.append(f"{col_name}: {null_count:,} nulls")

    if null_violations:
        result.failed_checks.append(f"null violations: {null_violations}")
    else:
        result.passed_checks.append("no nulls in non-nullable fields")

    # ── 6. Coordinate range checks ───────────────────────────────────────────
    out_of_bounds_lat = df.filter(
        (F.col("lat") < LAT_MIN) | (F.col("lat") > LAT_MAX)
    ).count()
    if out_of_bounds_lat == 0:
        result.passed_checks.append(
            f"all lat values within [{LAT_MIN}, {LAT_MAX}]"
        )
    else:
        result.failed_checks.append(
            f"lat out of bounds [{LAT_MIN}, {LAT_MAX}]: {out_of_bounds_lat:,} rows"
        )

    out_of_bounds_lon = df.filter(
        (F.col("lon") < LON_MIN) | (F.col("lon") > LON_MAX)
    ).count()
    if out_of_bounds_lon == 0:
        result.passed_checks.append(
            f"all lon values within [{LON_MIN}, {LON_MAX}]"
        )
    else:
        result.failed_checks.append(
            f"lon out of bounds [{LON_MIN}, {LON_MAX}]: {out_of_bounds_lon:,} rows"
        )

    # ── 7. Non-negative speed ────────────────────────────────────────────────
    negative_speed = df.filter(F.col("speed_ms") < SPEED_MIN).count()
    if negative_speed == 0:
        result.passed_checks.append("speed_ms is non-negative on all rows")
    else:
        result.failed_checks.append(
            f"speed_ms < {SPEED_MIN} on {negative_speed:,} rows"
        )

    # ── 8. source_file populated ─────────────────────────────────────────────
    empty_source = df.filter(
        F.col("source_file").isNull() | (F.col("source_file") == "")
    ).count()
    if empty_source == 0:
        result.passed_checks.append("source_file is populated on all rows")
    else:
        result.failed_checks.append(
            f"source_file is null or empty on {empty_source:,} rows"
        )

    # ── 9. bronze_key populated ──────────────────────────────────────────────
    empty_key = df.filter(
        F.col("bronze_key").isNull() | (F.col("bronze_key") == "")
    ).count()
    if empty_key == 0:
        result.passed_checks.append("bronze_key is populated on all rows")
    else:
        result.failed_checks.append(
            f"bronze_key is null or empty on {empty_key:,} rows"
        )

    # ── Log summary ──────────────────────────────────────────────────────────
    log.info(result.summary())

    return result
