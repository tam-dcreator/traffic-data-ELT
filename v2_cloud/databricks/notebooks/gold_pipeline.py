# Databricks notebook source
# MAGIC %md
# MAGIC # V2 Gold Pipeline — Silver frames → trajectory_summary Parquet
# MAGIC
# MAGIC **Execution model:** Databricks Serverless Compute
# MAGIC
# MAGIC **Flow:**
# MAGIC ```
# MAGIC S3 Silver frame Parquet
# MAGIC     ↓  spark.read.parquet (UC external location — no boto3, no keys)
# MAGIC     ↓  build_trajectory_summary(...)  — Spark impl of V1
# MAGIC     ↓                                   int_vehicle_trajectory_summary
# MAGIC     ↓  S3 Gold Parquet (trajectory_summary/test/)
# MAGIC     ↓  read-back
# MAGIC     ↓  validate_gold(...)  — strict on real Spark
# MAGIC ```
# MAGIC
# MAGIC **No temporary layer** — Silver → Spark → Gold is a direct S3-to-S3
# MAGIC transformation.  No UC volume extraction is needed for Gold.
# MAGIC
# MAGIC **Acceptance targets (representative sample):**
# MAGIC - Silver frame rows:   1,446,887
# MAGIC - Gold trajectories:   922
# MAGIC - SUM(frame_count):    1,446,887   (frame conservation)
# MAGIC - Grain (source_file, track_id) unique

# COMMAND ----------
# MAGIC %md ## 0. Package installation
# MAGIC
# MAGIC Install the shared `traffic-data-elt` wheel (pure stdlib runtime path).
# MAGIC The `v2_cloud/databricks/*` modules are synced separately to the UC
# MAGIC volume `code/` path and added to `sys.path` in Cell 1.

# COMMAND ----------

%pip install --no-deps /Volumes/workspace/default/v2_temp/wheels/traffic_data_elt-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ## 1. Configuration

# COMMAND ----------

import os
import sys
import uuid

# ── Make the v2_cloud Databricks modules importable ───────────────────────────
# The Databricks-specific modules under v2_cloud/databricks/ are NOT part of the
# wheel (they live outside src/).  They are synced to the UC volume `code/` path
# and added to sys.path here.  Namespace-package resolution handles the missing
# __init__.py files.  This is the same temporary development sync mechanism used
# by the Silver milestone (documented as technical debt in the README).
_V2_CODE_ROOT = "/Volumes/workspace/default/v2_temp/code"
if _V2_CODE_ROOT not in sys.path:
    sys.path.insert(0, _V2_CODE_ROOT)


def _cfg(name: str, default: str = "") -> str:
    """Read config from a notebook widget, then env var, then default."""
    try:
        val = dbutils.widgets.get(name)  # noqa: F821 - dbutils is injected
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(name, default)


# ── S3 layout ─────────────────────────────────────────────────────────────────
s3_bucket = _cfg("S3_BUCKET", "")
assert s3_bucket, (
    "S3_BUCKET must be set — use Databricks Secrets or cluster environment "
    "variables. Never hardcode credentials here."
)

silver_prefix = _cfg("S3_SILVER_PREFIX", "silver")
gold_prefix   = _cfg("S3_GOLD_PREFIX", "gold")

silver_s3_path = f"s3://{s3_bucket}/{silver_prefix}/pneuma/trajectories/test/"
gold_s3_path   = f"s3://{s3_bucket}/{gold_prefix}/pneuma/trajectory_summary/test/"

# ── Acceptance gates (V1 sample) ──────────────────────────────────────────────
EXPECTED_SILVER_FRAMES     = 1_446_887
EXPECTED_GOLD_TRAJECTORIES = 922

run_id = str(uuid.uuid4())[:8]

print(f"run_id:          {run_id}")
print(f"silver_s3_path:  {silver_s3_path}")
print(f"gold_s3_path:    {gold_s3_path}")

# COMMAND ----------
# MAGIC %md ## 2. Read Silver + build Gold trajectory summary + write Parquet

# COMMAND ----------

from v2_cloud.databricks.gold_transformer import write_gold

result = write_gold(
    spark=spark,
    silver_s3_path=silver_s3_path,
    gold_s3_path=gold_s3_path,
    run_id=run_id,
    coalesce_partitions=1,
)

print(f"status:                {result.status}")
print(f"run_id:                {result.run_id}")
print(f"silver frame rows:     {result.silver_frame_count:,}")
print(f"gold trajectories:     {result.gold_trajectory_count:,}")
print(f"sum(frame_count):      {result.sum_frame_count:,}")
print(f"frames conserved:      {result.frames_conserved}")
print(f"gold path:             {result.gold_path}")
print(f"elapsed:               {(result.end_time - result.start_time).total_seconds():.1f}s")

if result.status != "success":
    raise RuntimeError(f"Gold write failed: {result.error}")

# ── Pre-validation acceptance gates ───────────────────────────────────────────
if result.silver_frame_count != EXPECTED_SILVER_FRAMES:
    raise RuntimeError(
        f"Silver input mismatch: expected {EXPECTED_SILVER_FRAMES:,} frame rows, "
        f"got {result.silver_frame_count:,}. STOP — diagnose Silver before Gold."
    )

if result.gold_trajectory_count != EXPECTED_GOLD_TRAJECTORIES:
    raise RuntimeError(
        f"Gold trajectory count mismatch: expected {EXPECTED_GOLD_TRAJECTORIES:,}, "
        f"got {result.gold_trajectory_count:,}. STOP — diagnose grain/grouping."
    )

if not result.frames_conserved:
    raise RuntimeError(
        f"FRAME CONSERVATION FAILURE: SUM(frame_count)={result.sum_frame_count:,} "
        f"!= silver frames {result.silver_frame_count:,}. "
        f"Indicates frame loss, duplication, or incorrect grouping. STOP."
    )

print("✓ Pre-validation acceptance gates passed")

# COMMAND ----------
# MAGIC %md ## 3. Read back and validate the persisted Gold Parquet (strict)

# COMMAND ----------

from v2_cloud.databricks.gold_validator import validate_gold

# Read-back row count vs write count.
readback_df = spark.read.parquet(gold_s3_path)
readback_count = readback_df.count()
print(f"write row count:      {result.gold_trajectory_count:,}")
print(f"read-back row count:  {readback_count:,}")
assert readback_count == result.gold_trajectory_count, (
    f"read-back count {readback_count:,} != write count "
    f"{result.gold_trajectory_count:,}"
)
print("✓ read-back row count matches write count")

validation = validate_gold(
    spark=spark,
    gold_path=gold_s3_path,
    expected_trajectory_count=EXPECTED_GOLD_TRAJECTORIES,
    expected_frame_sum=EXPECTED_SILVER_FRAMES,
)

print(validation.summary())

if not validation.passed:
    raise RuntimeError(
        "Gold validation FAILED:\n"
        + "\n".join(f"  ✗ {c}" for c in validation.failed_checks)
    )

print("✓ All Gold validation checks passed on the persisted dataset")

# COMMAND ----------
# MAGIC %md ## 4. Optional — V1/V2 parity export
# MAGIC
# MAGIC Full field-level parity against the V1 dbt
# MAGIC `int_vehicle_trajectory_summary` is performed **off-cluster** by the
# MAGIC integration harness, which has access to the local V1 PostgreSQL
# MAGIC warehouse.  This cell exports the Gold rows as a compact JSON so the
# MAGIC harness can compare them without re-running the job.

# COMMAND ----------

import json

gold_rows = [row.asDict() for row in spark.read.parquet(gold_s3_path).collect()]
export_path = f"/Volumes/workspace/default/v2_temp/exports/gold_{run_id}.json"
dbutils.fs.mkdirs("dbfs:/Volumes/workspace/default/v2_temp/exports")
with open(export_path, "w") as fh:
    json.dump(gold_rows, fh)
print(f"✓ exported {len(gold_rows):,} Gold rows to {export_path}")

# COMMAND ----------
# MAGIC %md ## 5. Observability summary

# COMMAND ----------

elapsed_total = (result.end_time - result.start_time).total_seconds()

print("=" * 65)
print("V2 GOLD PIPELINE — COMPLETE")
print("=" * 65)
print(f"  run_id:               {run_id}")
print(f"  silver input path:    {result.silver_path}")
print(f"  gold output path:     {result.gold_path}")
print(f"  silver frame count:   {result.silver_frame_count:,}")
print(f"  gold trajectories:    {result.gold_trajectory_count:,}")
print(f"  sum(frame_count):     {result.sum_frame_count:,}")
print(f"  frames conserved:     {result.frames_conserved}")
print(f"  parquet compression:  snappy")
print(f"  coalesce partitions:  1")
print(f"  transform+write time: {elapsed_total:.1f}s")
print(f"  validation:           {'PASSED' if validation.passed else 'FAILED'}")
print(f"  start_ts:             {result.start_time.isoformat()}")
print(f"  end_ts:               {result.end_time.isoformat()}")
print("=" * 65)
