"""Gold transformer: S3 Silver frame Parquet → trajectory-summary Parquet.

Responsibility
--------------
This module reads the normalised frame-level Silver dataset from S3 and
aggregates it into the trajectory-level ``trajectory_summary`` Gold dataset,
then writes the result back to S3 Gold as Parquet.

It is the Spark implementation of the V1 dbt model
``intermediate.int_vehicle_trajectory_summary``.  The semantics are documented
in ``GOLD_CONTRACT.md`` and reproduced faithfully here — the V1 SQL is the
source of truth.

Design
------
- ``build_trajectory_summary(silver_df)`` is a **pure** Spark DataFrame
  transformation: Silver frame DataFrame in, Gold summary DataFrame out.  It
  contains all business logic and is unit-testable with a real SparkSession
  (Databricks) or skipped locally when PySpark is absent.
- ``write_gold(spark, silver_path, gold_path)`` orchestrates read → transform →
  write and returns observability metadata.  It contains no aggregation logic.
- Native Spark aggregations only — no Python UDFs.
- Parity-critical: frame columns are rounded to the same decimal places as V1
  staging (``stg_vehicle_trajectories``) **before** aggregation.

Aggregation mapping (V1 → Spark) — see GOLD_CONTRACT.md §4.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING

from traffic_data_elt.utils import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = get_logger(__name__)


@dataclass
class GoldWriteResult:
    """Outcome metadata for a completed Gold write."""

    silver_path: str
    gold_path: str
    silver_frame_count: int
    gold_trajectory_count: int
    sum_frame_count: int
    ingested_at: datetime.datetime
    start_time: datetime.datetime
    end_time: datetime.datetime
    status: str  # "success" | "failed"
    run_id: str | None = None
    error: str | None = None

    @property
    def frames_conserved(self) -> bool:
        """True when SUM(frame_count) == silver frame row count."""
        return self.sum_frame_count == self.silver_frame_count


# ---------------------------------------------------------------------------
# Pure transformation
# ---------------------------------------------------------------------------


def build_trajectory_summary(silver_df: "DataFrame") -> "DataFrame":
    """Aggregate frame-level Silver rows into the Gold trajectory summary.

    Pure DataFrame-in / DataFrame-out transformation.  Reproduces V1
    ``int_vehicle_trajectory_summary`` semantics exactly (see GOLD_CONTRACT.md).

    Steps
    -----
    1. Apply V1 staging rounding to frame columns (coords 6 d.p., kinematics
       4 d.p.) and defensively normalise ``vehicle_type`` (trim + lower).
    2. Drop rows with nulls in the V1 staging mandatory columns.
    3. Aggregate per ``(source_file, track_id)`` using native Spark functions.
    4. Attach first/last coordinates by ``timestamp_s`` via an ordered window.
    5. Project the 19 Gold columns in the contract order.

    Parameters
    ----------
    silver_df:
        Silver frame-level DataFrame (schema per ``silver_schema``).

    Returns
    -------
    DataFrame
        Gold trajectory-summary DataFrame (schema per ``gold_schema``),
        one row per ``(source_file, track_id)``.
    """
    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as F  # noqa: PLC0415

    from traffic_data_elt.databricks.schemas.gold_schema import (  # noqa: PLC0415
        COORD_ROUND_DP,
        GOLD_FIELD_NAMES,
        KINEMATIC_ROUND_DP,
    )

    # ── 1. Staging-equivalent frame preparation ─────────────────────────────
    # Mirror stg_vehicle_trajectories: normalise vehicle_type; round coords to
    # 6 d.p. and kinematics to 4 d.p.  traveled_d_m / avg_speed_ms are NOT
    # rounded (V1 carries them through unchanged from raw).
    prepared = (
        silver_df.select(
            "source_file",
            "track_id",
            F.trim(F.lower(F.col("vehicle_type"))).alias("vehicle_type"),
            F.col("traveled_d_m"),
            F.col("avg_speed_ms"),
            F.round(F.col("lat"), COORD_ROUND_DP).alias("lat"),
            F.round(F.col("lon"), COORD_ROUND_DP).alias("lon"),
            F.round(F.col("speed_ms"), KINEMATIC_ROUND_DP).alias("speed_ms"),
            F.round(F.col("lon_acc_ms2"), KINEMATIC_ROUND_DP).alias("lon_acc_ms2"),
            F.round(F.col("lat_acc_ms2"), KINEMATIC_ROUND_DP).alias("lat_acc_ms2"),
            F.col("timestamp_s"),
        )
        # V1 staging null filter (mandatory columns).
        .na.drop(subset=[
            "track_id", "vehicle_type", "lat", "lon", "speed_ms", "timestamp_s",
        ])
    )

    # ── 4. First/last coordinates by time (V1 array_agg order asc/desc) ──────
    # Deterministic ordering: timestamp then lat/lon as a tie-break (timestamps
    # are unique per track in this dataset, so the tie-break never triggers).
    w_asc = (
        Window.partitionBy("source_file", "track_id")
        .orderBy(F.col("timestamp_s").asc(), F.col("lat").asc(), F.col("lon").asc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    w_desc = (
        Window.partitionBy("source_file", "track_id")
        .orderBy(F.col("timestamp_s").desc(), F.col("lat").desc(), F.col("lon").desc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    with_ends = (
        prepared
        .withColumn("start_lat", F.first(F.col("lat")).over(w_asc))
        .withColumn("start_lon", F.first(F.col("lon")).over(w_asc))
        .withColumn("end_lat", F.first(F.col("lat")).over(w_desc))
        .withColumn("end_lon", F.first(F.col("lon")).over(w_desc))
    )

    # ── 3. Aggregate per trajectory ──────────────────────────────────────────
    agg = (
        with_ends.groupBy("source_file", "track_id")
        .agg(
            F.min("vehicle_type").alias("vehicle_type"),
            F.count(F.lit(1)).alias("frame_count"),
            F.min("timestamp_s").alias("start_time_s"),
            F.max("timestamp_s").alias("end_time_s"),
            (F.max("timestamp_s") - F.min("timestamp_s")).alias("duration_s"),
            F.min("traveled_d_m").alias("traveled_d_m"),
            F.min("avg_speed_ms").alias("avg_speed_ms"),
            F.max("speed_ms").alias("max_speed_ms"),
            F.min("speed_ms").alias("min_speed_ms"),
            F.avg("lon_acc_ms2").alias("avg_lon_acc_ms2"),
            F.avg("lat_acc_ms2").alias("avg_lat_acc_ms2"),
            F.max(F.abs(F.col("lon_acc_ms2"))).alias("max_lon_acc_ms2"),
            F.max(F.abs(F.col("lat_acc_ms2"))).alias("max_lat_acc_ms2"),
            # Coordinates are constant within a group thanks to the window above.
            F.first("start_lat").alias("start_lat"),
            F.first("start_lon").alias("start_lon"),
            F.first("end_lat").alias("end_lat"),
            F.first("end_lon").alias("end_lon"),
        )
    )

    # ── 5. Project the 19 Gold columns in contract order ─────────────────────
    # frame_count comes back as long from count(); cast explicitly for the
    # declared LongType contract.
    gold = agg.select(
        "source_file",
        "track_id",
        "vehicle_type",
        F.col("frame_count").cast("long").alias("frame_count"),
        "start_time_s",
        "end_time_s",
        "duration_s",
        "traveled_d_m",
        "avg_speed_ms",
        "max_speed_ms",
        "min_speed_ms",
        "avg_lon_acc_ms2",
        "avg_lat_acc_ms2",
        "max_lon_acc_ms2",
        "max_lat_acc_ms2",
        "start_lat",
        "start_lon",
        "end_lat",
        "end_lon",
    )

    # Guard: projection order must equal the contract.
    assert gold.columns == GOLD_FIELD_NAMES, (
        f"Gold column order {gold.columns} != contract {GOLD_FIELD_NAMES}"
    )
    return gold


# ---------------------------------------------------------------------------
# Orchestration: read → transform → write
# ---------------------------------------------------------------------------


def write_gold(
    spark: "SparkSession",
    silver_s3_path: str,
    gold_s3_path: str,
    *,
    run_id: str | None = None,
    coalesce_partitions: int | None = 1,
) -> GoldWriteResult:
    """Read Silver, build the trajectory summary, and write Gold Parquet.

    Parameters
    ----------
    spark:
        Active SparkSession.
    silver_s3_path:
        Full S3 path to the Silver Parquet directory
        (e.g. ``s3://bucket/silver/pneuma/trajectories/test/``).
    gold_s3_path:
        Full S3 path for Gold Parquet output
        (e.g. ``s3://bucket/gold/pneuma/trajectory_summary/test/``).
    run_id:
        Optional run identifier for observability.
    coalesce_partitions:
        Number of output Parquet files.  ``1`` for the small test fixture to
        avoid tiny-file proliferation.  ``None`` disables coalesce.

    Returns
    -------
    GoldWriteResult
        Counts, frame-conservation, timing, and status.
    """
    from pyspark.sql import functions as F  # noqa: PLC0415

    start_time = datetime.datetime.now(tz=datetime.timezone.utc)
    ingested_at = start_time

    log.info(
        "gold write starting: silver=%s gold=%s run_id=%s",
        silver_s3_path,
        gold_s3_path,
        run_id,
    )

    # ── Read Silver directly from S3 (UC external location) ──────────────────
    try:
        silver_df = spark.read.parquet(silver_s3_path)
    except Exception as exc:  # noqa: BLE001
        end_time = datetime.datetime.now(tz=datetime.timezone.utc)
        return GoldWriteResult(
            silver_path=silver_s3_path,
            gold_path=gold_s3_path,
            silver_frame_count=0,
            gold_trajectory_count=0,
            sum_frame_count=0,
            ingested_at=ingested_at,
            start_time=start_time,
            end_time=end_time,
            status="failed",
            run_id=run_id,
            error=f"cannot read Silver at {silver_s3_path}: {exc}",
        )

    silver_frame_count = silver_df.count()
    log.info("silver frame rows: %d", silver_frame_count)

    # ── Transform ────────────────────────────────────────────────────────────
    gold_df = build_trajectory_summary(silver_df)

    if coalesce_partitions is not None and coalesce_partitions > 0:
        gold_df = gold_df.coalesce(coalesce_partitions)

    # ── Write Parquet ────────────────────────────────────────────────────────
    # Serverless compute does not support DataFrame.cache()/persist(), so we
    # write first and then derive counts from the persisted Gold on read-back.
    # This is also stronger evidence than an in-memory count: the reported
    # counts describe the data actually landed in S3.
    log.info("writing gold rows to %s", gold_s3_path)
    (
        gold_df.write.format("parquet")
        .option("compression", "snappy")
        .mode("overwrite")
        .save(gold_s3_path)
    )

    # ── Counts from the persisted Gold Parquet ───────────────────────────────
    persisted = spark.read.parquet(gold_s3_path)
    gold_trajectory_count = persisted.count()
    sum_frame_count = (
        persisted.agg(F.sum("frame_count").alias("s")).collect()[0]["s"] or 0
    )
    log.info(
        "gold trajectories: %d ; sum(frame_count): %d",
        gold_trajectory_count,
        sum_frame_count,
    )

    end_time = datetime.datetime.now(tz=datetime.timezone.utc)
    elapsed = (end_time - start_time).total_seconds()
    log.info("gold write complete in %.1f s: %s", elapsed, gold_s3_path)

    return GoldWriteResult(
        silver_path=silver_s3_path,
        gold_path=gold_s3_path,
        silver_frame_count=silver_frame_count,
        gold_trajectory_count=gold_trajectory_count,
        sum_frame_count=int(sum_frame_count),
        ingested_at=ingested_at,
        start_time=start_time,
        end_time=end_time,
        status="success",
        run_id=run_id,
    )
