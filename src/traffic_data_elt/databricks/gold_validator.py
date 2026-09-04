"""Gold output validator for V2 Databricks processing.

Validates the written Gold ``trajectory_summary`` Parquet dataset before
signalling pipeline success.  The V1 model ``int_vehicle_trajectory_summary``
and its dbt tests are the primary contract (see ``GOLD_CONTRACT.md``).

Checks
------
1. Gold Parquet is readable by Spark.
2. Row count > 0.
3. (Optional) Expected trajectory count matches (e.g. 922 for the sample).
4. Schema field names match the Gold contract exactly.
5. All field types match the Gold schema — strict failure on real Spark.
6. No nulls in non-nullable fields (all 19 columns).
7. Trajectory key ``(source_file, track_id)`` is unique.
8. ``frame_count >= 1`` on every row.
9. (Optional) Frame conservation: SUM(frame_count) == silver frame count.
10. ``start_time_s <= end_time_s`` on every row.
11. ``duration_s >= 0`` on every row.
12. ``traveled_d_m >= 0`` on every row.
13. ``min_speed_ms >= 0`` and ``max_speed_ms >= 0`` on every row.
14. ``source_file`` populated (non-empty) on every row.

Local unit tests may stub PySpark; data-quality checks are skipped with a
warning when PySpark is unavailable.  Real Databricks execution is strict.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from traffic_data_elt.utils import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger(__name__)


class GoldValidationError(RuntimeError):
    """Raised for fatal configuration or framework errors during validation."""


@dataclass
class GoldValidationResult:
    """Outcome of all Gold validation checks."""

    gold_path: str
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
            f"Gold validation: {'PASSED' if self.passed else 'FAILED'}",
            f"  path:         {self.gold_path}",
            f"  row_count:    {self.row_count:,}",
            f"  passed:       {len(self.passed_checks)}",
            f"  failed:       {len(self.failed_checks)}",
        ]
        for msg in self.failed_checks:
            lines.append(f"  FAIL  {msg}")
        for msg in self.warnings:
            lines.append(f"  WARN  {msg}")
        return "\n".join(lines)


def validate_gold(
    spark: "SparkSession",
    gold_path: str,
    *,
    expected_trajectory_count: int | None = None,
    expected_frame_sum: int | None = None,
) -> GoldValidationResult:
    """Run all Gold validation checks against the persisted Parquet.

    Parameters
    ----------
    spark:
        Active SparkSession.
    gold_path:
        Full S3 path to the Gold Parquet output directory.
    expected_trajectory_count:
        When supplied, the Gold row count is compared against this value
        (e.g. 922 for the sample).  A mismatch is a failure.
    expected_frame_sum:
        When supplied, ``SUM(frame_count)`` is compared against this value
        (the Silver frame row count).  A mismatch is a failure — this is the
        strongest Silver→Gold conservation invariant.

    Returns
    -------
    GoldValidationResult
        All check outcomes.  Check ``.passed`` before proceeding.
    """
    from traffic_data_elt.databricks.schemas.gold_schema import (
        GOLD_FIELD_NAMES,
        GOLD_GRAIN_KEYS,
        GOLD_NON_NULLABLE_FIELDS,
        get_gold_schema,
    )

    result = GoldValidationResult(gold_path=gold_path, row_count=0)

    # ── 1. Read back Gold Parquet ────────────────────────────────────────────
    try:
        df = spark.read.parquet(gold_path)
    except Exception as exc:  # noqa: BLE001
        result.failed_checks.append(
            f"cannot read Gold Parquet at {gold_path}: {exc}"
        )
        log.error("Gold read-back failed: %s", exc)
        return result

    result.passed_checks.append("Gold Parquet is readable")

    # ── 2. Row count > 0 ─────────────────────────────────────────────────────
    row_count = df.count()
    result.row_count = row_count
    if row_count > 0:
        result.passed_checks.append(f"row_count > 0 (actual: {row_count:,})")
    else:
        result.failed_checks.append("row_count == 0 — Gold dataset is empty")
        return result

    # ── 3. Expected trajectory count ─────────────────────────────────────────
    if expected_trajectory_count is not None:
        if row_count == expected_trajectory_count:
            result.passed_checks.append(
                f"trajectory count matches expected {expected_trajectory_count:,}"
            )
        else:
            result.failed_checks.append(
                f"trajectory count FAILED: expected {expected_trajectory_count:,}, "
                f"got {row_count:,} (delta: {row_count - expected_trajectory_count:+,})"
            )
            log.error(
                "TRAJECTORY COUNT FAILURE: expected=%d actual=%d",
                expected_trajectory_count,
                row_count,
            )

    # ── 4. Schema field names ────────────────────────────────────────────────
    actual_field_names = [f.name for f in df.schema.fields]
    if actual_field_names == GOLD_FIELD_NAMES:
        result.passed_checks.append("schema field names match Gold contract")
    else:
        missing = set(GOLD_FIELD_NAMES) - set(actual_field_names)
        extra = set(actual_field_names) - set(GOLD_FIELD_NAMES)
        result.failed_checks.append(
            f"schema mismatch — missing: {sorted(missing)}, extra: {sorted(extra)}"
        )

    # ── 5. Field type check (strict on real Spark) ───────────────────────────
    _pyspark_available = True
    try:
        from pyspark.sql import functions as F  # noqa: PLC0415
    except ModuleNotFoundError:
        _pyspark_available = False

    if _pyspark_available:
        try:
            gold_schema = get_gold_schema()
            schema_type_map = {f.name: f.dataType for f in gold_schema.fields}
            actual_type_map = {f.name: f.dataType for f in df.schema.fields}
            type_mismatches: list[str] = []
            for name in GOLD_FIELD_NAMES:
                if name not in actual_type_map or name not in schema_type_map:
                    continue
                if type(schema_type_map[name]) is not type(actual_type_map[name]):
                    type_mismatches.append(
                        f"{name}: expected {type(schema_type_map[name]).__name__}, "
                        f"got {type(actual_type_map[name]).__name__}"
                    )
            if type_mismatches:
                result.failed_checks.append(f"field type mismatches: {type_mismatches}")
            else:
                result.passed_checks.append("all field types match Gold schema")
        except Exception as exc:  # noqa: BLE001
            result.failed_checks.append(
                f"schema type check raised an unexpected error: {exc}"
            )
            log.error("Gold schema type check failed unexpectedly: %s", exc)
    else:
        result.warnings.append(
            "pyspark not installed locally — field type check skipped "
            "(runs strictly on real Databricks execution)"
        )

    # ── Data-quality checks require real Spark ───────────────────────────────
    if not _pyspark_available:
        result.failed_checks.append(
            "pyspark unavailable locally — cannot execute Gold data quality checks. "
            "Run on Databricks for full validation."
        )
        log.error("pyspark unavailable: Gold data checks skipped")
        return result

    # ── 6. Null checks on non-nullable fields ────────────────────────────────
    null_violations: list[str] = []
    for col_name in GOLD_NON_NULLABLE_FIELDS:
        if col_name not in actual_field_names:
            continue
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            null_violations.append(f"{col_name}: {null_count:,} nulls")
    if null_violations:
        result.failed_checks.append(f"null violations: {null_violations}")
    else:
        result.passed_checks.append("no nulls in non-nullable fields")

    # ── 7. Trajectory key uniqueness ─────────────────────────────────────────
    total = df.count()
    distinct_keys = df.select(*GOLD_GRAIN_KEYS).distinct().count()
    if distinct_keys == total:
        result.passed_checks.append(
            f"trajectory key {tuple(GOLD_GRAIN_KEYS)} is unique ({total:,} rows)"
        )
    else:
        dupes = total - distinct_keys
        result.failed_checks.append(
            f"trajectory key not unique: {dupes:,} duplicate key(s) "
            f"({total:,} rows, {distinct_keys:,} distinct)"
        )

    # ── 8. frame_count >= 1 ──────────────────────────────────────────────────
    bad_frame_count = df.filter(F.col("frame_count") < 1).count()
    if bad_frame_count == 0:
        result.passed_checks.append("frame_count >= 1 on all rows")
    else:
        result.failed_checks.append(f"frame_count < 1 on {bad_frame_count:,} rows")

    # ── 9. Frame conservation ────────────────────────────────────────────────
    if expected_frame_sum is not None:
        actual_sum = df.agg(F.sum("frame_count").alias("s")).collect()[0]["s"] or 0
        actual_sum = int(actual_sum)
        if actual_sum == expected_frame_sum:
            result.passed_checks.append(
                f"frame conservation: SUM(frame_count) == {expected_frame_sum:,}"
            )
        else:
            result.failed_checks.append(
                f"frame conservation FAILED: SUM(frame_count)={actual_sum:,}, "
                f"expected {expected_frame_sum:,} "
                f"(delta: {actual_sum - expected_frame_sum:+,})"
            )
            log.error(
                "FRAME CONSERVATION FAILURE: sum=%d expected=%d",
                actual_sum,
                expected_frame_sum,
            )

    # ── 10. start_time_s <= end_time_s ───────────────────────────────────────
    bad_time = df.filter(F.col("start_time_s") > F.col("end_time_s")).count()
    if bad_time == 0:
        result.passed_checks.append("start_time_s <= end_time_s on all rows")
    else:
        result.failed_checks.append(
            f"start_time_s > end_time_s on {bad_time:,} rows"
        )

    # ── 11. duration_s >= 0 ──────────────────────────────────────────────────
    bad_duration = df.filter(F.col("duration_s") < 0).count()
    if bad_duration == 0:
        result.passed_checks.append("duration_s >= 0 on all rows")
    else:
        result.failed_checks.append(f"duration_s < 0 on {bad_duration:,} rows")

    # ── 12. traveled_d_m >= 0 ────────────────────────────────────────────────
    bad_distance = df.filter(F.col("traveled_d_m") < 0).count()
    if bad_distance == 0:
        result.passed_checks.append("traveled_d_m >= 0 on all rows")
    else:
        result.failed_checks.append(f"traveled_d_m < 0 on {bad_distance:,} rows")

    # ── 13. speed metrics >= 0 ───────────────────────────────────────────────
    bad_speed = df.filter(
        (F.col("min_speed_ms") < 0) | (F.col("max_speed_ms") < 0)
    ).count()
    if bad_speed == 0:
        result.passed_checks.append("min/max speed >= 0 on all rows")
    else:
        result.failed_checks.append(
            f"negative speed metric on {bad_speed:,} rows"
        )

    # ── 14. source_file populated ────────────────────────────────────────────
    empty_source = df.filter(
        F.col("source_file").isNull() | (F.col("source_file") == "")
    ).count()
    if empty_source == 0:
        result.passed_checks.append("source_file is populated on all rows")
    else:
        result.failed_checks.append(
            f"source_file null or empty on {empty_source:,} rows"
        )

    log.info(result.summary())
    return result
