# Databricks notebook source
# MAGIC %md
# MAGIC # V2 Gold Pipeline — Silver frames → trajectory_summary Parquet
# MAGIC
# MAGIC **Execution model:** Databricks Serverless Compute
# MAGIC
# MAGIC **Flow:**
# MAGIC ```
# MAGIC S3 Silver frame Parquet  (SILVER_INPUT_PATH)
# MAGIC     ↓  spark.read.parquet (UC external location — no boto3, no keys)
# MAGIC     ↓  build_trajectory_summary(...)  — Spark impl of V1
# MAGIC     ↓                                   int_vehicle_trajectory_summary
# MAGIC     ↓  S3 Gold Parquet  (GOLD_OUTPUT_PATH)
# MAGIC     ↓  read-back
# MAGIC     ↓  validate_gold(...)  — strict on real Spark
# MAGIC ```
# MAGIC
# MAGIC **Configuration-driven.** Input/output paths are explicit job parameters
# MAGIC (no hardcoded `/test/` prefix). Runtime imports come from the installed
# MAGIC `traffic_data_elt` wheel (no UC-volume `sys.path` sync). Fixture counts are
# MAGIC only enforced when supplied (integration mode); production validates
# MAGIC invariants.

# COMMAND ----------
# MAGIC %md ## 0. Install the versioned wheel

# COMMAND ----------

dbutils.widgets.text("WHEEL_PATH", "")
_wheel = dbutils.widgets.get("WHEEL_PATH") or \
    "/Volumes/workspace/default/v2_artifacts/wheels/traffic_data_elt-latest-py3-none-any.whl"
%pip install --no-deps {_wheel}

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ## 1. Configuration

# COMMAND ----------

import os
import uuid


def _cfg(name: str, default: str = "") -> str:
    try:
        val = dbutils.widgets.get(name)  # noqa: F821
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(name, default)


# ── Input / output paths (REQUIRED, explicit — no /test/ default) ─────────────
silver_input_path = _cfg("SILVER_INPUT_PATH", "")
gold_output_path = _cfg("GOLD_OUTPUT_PATH", "")
assert silver_input_path, "SILVER_INPUT_PATH is required (full s3:// path)."
assert gold_output_path, "GOLD_OUTPUT_PATH is required (full s3:// path)."

# ── Fixture-vs-production validation ──────────────────────────────────────────
_exp_silver = _cfg("EXPECTED_SILVER_FRAMES", "")
_exp_gold = _cfg("EXPECTED_GOLD_TRAJECTORIES", "")
expected_silver_frames = int(_exp_silver) if _exp_silver else None
expected_gold_trajectories = int(_exp_gold) if _exp_gold else None
fixture_mode = expected_silver_frames is not None or expected_gold_trajectories is not None

run_id = str(uuid.uuid4())[:8]

print(f"run_id:             {run_id}")
print(f"silver_input_path:  {silver_input_path}")
print(f"gold_output_path:   {gold_output_path}")
print(f"validation:         {'fixture' if fixture_mode else 'production (invariants only)'}")

# COMMAND ----------
# MAGIC %md ## 2. Read Silver + build Gold trajectory summary + write Parquet

# COMMAND ----------

from traffic_data_elt.databricks.gold_transformer import write_gold

result = write_gold(
    spark=spark,
    silver_s3_path=silver_input_path,
    gold_s3_path=gold_output_path,
    run_id=run_id,
    coalesce_partitions=1,
)

print(f"status:                {result.status}")
print(f"silver frame rows:     {result.silver_frame_count:,}")
print(f"gold trajectories:     {result.gold_trajectory_count:,}")
print(f"sum(frame_count):      {result.sum_frame_count:,}")
print(f"gold path:             {result.gold_path}")

if result.status != "success":
    raise RuntimeError(f"Gold write failed: {result.error}")

# ── Invariants (always) ───────────────────────────────────────────────────────
if result.silver_frame_count <= 0:
    raise RuntimeError("Silver input is empty — STOP.")
if result.gold_trajectory_count <= 0:
    raise RuntimeError("Gold produced no trajectories — STOP.")
if result.sum_frame_count != result.silver_frame_count:
    raise RuntimeError(
        f"FRAME CONSERVATION FAILURE: SUM(frame_count)={result.sum_frame_count:,} "
        f"!= silver frames {result.silver_frame_count:,}."
    )

# ── Fixture expectations (integration mode only) ──────────────────────────────
if fixture_mode:
    if expected_silver_frames is not None and result.silver_frame_count != expected_silver_frames:
        raise RuntimeError(
            f"fixture expected {expected_silver_frames:,} silver frames, "
            f"got {result.silver_frame_count:,}."
        )
    if expected_gold_trajectories is not None and result.gold_trajectory_count != expected_gold_trajectories:
        raise RuntimeError(
            f"fixture expected {expected_gold_trajectories:,} gold trajectories, "
            f"got {result.gold_trajectory_count:,}."
        )
print("✓ Pre-validation gates passed")

# COMMAND ----------
# MAGIC %md ## 3. Read back and validate the persisted Gold Parquet (strict)

# COMMAND ----------

from traffic_data_elt.databricks.gold_validator import validate_gold

readback_count = spark.read.parquet(gold_output_path).count()
assert readback_count == result.gold_trajectory_count, (
    f"read-back count {readback_count:,} != write count {result.gold_trajectory_count:,}"
)
print("✓ read-back row count matches write count")

# expected_* are None in production → validate_gold enforces invariants only.
validation = validate_gold(
    spark=spark,
    gold_path=gold_output_path,
    expected_trajectory_count=expected_gold_trajectories,
    expected_frame_sum=expected_silver_frames,
)
print(validation.summary())
if not validation.passed:
    raise RuntimeError(
        "Gold validation FAILED:\n" + "\n".join(f"  ✗ {c}" for c in validation.failed_checks)
    )
print("✓ All Gold validation checks passed on the persisted dataset")

# COMMAND ----------
# MAGIC %md ## 4. Optional — V1/V2 parity export
# MAGIC
# MAGIC Off-cluster parity harness compares against the V1 warehouse. Export path
# MAGIC is configurable; skipped when EXPORT_DIR is unset.

# COMMAND ----------

import json

export_dir = _cfg("EXPORT_DIR", "")
if export_dir:
    gold_rows = [row.asDict() for row in spark.read.parquet(gold_output_path).collect()]
    dbutils.fs.mkdirs(f"dbfs:{export_dir}")
    export_path = f"{export_dir}/gold_{run_id}.json"
    with open(export_path, "w") as fh:
        json.dump(gold_rows, fh)
    print(f"✓ exported {len(gold_rows):,} Gold rows to {export_path}")
else:
    print("EXPORT_DIR unset — skipping parity export.")

# COMMAND ----------
# MAGIC %md ## 5. Observability summary

# COMMAND ----------

print("V2 GOLD PIPELINE — COMPLETE")
print(f"  run_id:               {run_id}")
print(f"  silver input path:    {result.silver_path}")
print(f"  gold output path:     {result.gold_path}")
print(f"  silver frame count:   {result.silver_frame_count:,}")
print(f"  gold trajectories:    {result.gold_trajectory_count:,}")
print(f"  sum(frame_count):     {result.sum_frame_count:,}")
print(f"  validation:           {'PASSED' if validation.passed else 'FAILED'}")
