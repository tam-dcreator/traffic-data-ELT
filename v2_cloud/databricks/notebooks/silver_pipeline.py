# Databricks notebook source
# MAGIC %md
# MAGIC # V2 Silver Pipeline — pNEUMA Bronze ZIP → Silver Parquet
# MAGIC
# MAGIC **Execution model:** Databricks Serverless Compute
# MAGIC
# MAGIC **Flow:**
# MAGIC ```
# MAGIC S3 Bronze ZIP
# MAGIC     ↓  download to UC managed volume  (/Volumes/<catalog>/<schema>/v2_temp/runs/<run_id>/source/)
# MAGIC     ↓  unzip → pnemas.csv             (/Volumes/<catalog>/<schema>/v2_temp/runs/<run_id>/extracted/)
# MAGIC     ↓  PneumaExtractor.extract_from_lines(...)
# MAGIC     ↓  Spark DataFrame (explicit schema)
# MAGIC     ↓  S3 Silver Parquet
# MAGIC     ↓  validation (schema, types, row count, coordinates, V1 parity)
# MAGIC     ↓  cleanup — delete run dir from UC volume
# MAGIC ```
# MAGIC
# MAGIC **Validation modes:**
# MAGIC - production: invariants only (row_count > 0, schema, coordinates, ...)
# MAGIC - fixture/integration: pass `EXPECTED_FRAME_ROWS` to additionally assert
# MAGIC   the known sample counts (e.g. the pNEUMA sample fixture in
# MAGIC   `tests/fixtures/pneuma_sample_expectations.toml`).

# COMMAND ----------
# MAGIC %md ## 0. Install the versioned wheel
# MAGIC
# MAGIC Install the deployed `traffic-data-elt` wheel (see
# MAGIC `scripts/deploy_databricks_artifact.py`). `WHEEL_PATH` is a job parameter.
# MAGIC
# MAGIC ### `--no-deps` rationale
# MAGIC The Silver runtime path (`PneumaExtractor` + logging) uses only the
# MAGIC Python standard library.  pandas / boto3 are already present on the
# MAGIC serverless runtime.  Installing with `--no-deps` avoids re-resolving
# MAGIC (and possibly downgrading) the runtime's pre-installed packages.

# COMMAND ----------

# Install the deployed, versioned wheel WITHOUT dependencies — the Silver code
# path is pure stdlib and the serverless runtime already provides pandas/boto3.
# WHEEL_PATH is a job parameter (see scripts/deploy_databricks_artifact.py).
dbutils.widgets.text("WHEEL_PATH", "")
_wheel = dbutils.widgets.get("WHEEL_PATH") or \
    "/Volumes/workspace/default/v2_artifacts/wheels/traffic_data_elt-latest-py3-none-any.whl"
%pip install --no-deps {_wheel}

# COMMAND ----------

# Restart the Python interpreter so the freshly installed package is importable.
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ## 1. Configuration
# MAGIC
# MAGIC AWS credentials are resolved via the standard boto3 provider chain
# MAGIC (IAM role, environment, or Databricks Secrets).  Do NOT hardcode them.
# MAGIC
# MAGIC Reference secrets via `dbutils.secrets.get(scope, key)`:
# MAGIC ```python
# MAGIC s3_bucket = dbutils.secrets.get(scope="v2-config", key="s3-bucket")
# MAGIC ```

# COMMAND ----------

import os
import uuid

# Runtime modules are imported from the installed `traffic_data_elt` wheel
# (traffic_data_elt.databricks.*). No UC-volume source sync / sys.path shim.
# S3 access uses the Unity Catalog external-location credential (no boto3
# creds, no AWS_REGION needed on this compute).


def _cfg(name: str, default: str = "") -> str:
    """Read config from a notebook widget, then env var, then default.

    Widgets let a Jobs API run pass parameters; env vars support interactive
    or databricks-connect runs.  Secrets should be used for anything sensitive
    (this pipeline needs only non-secret bucket/region values).
    """
    try:
        val = dbutils.widgets.get(name)  # noqa: F821 - dbutils is injected
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(name, default)


# ── S3 bucket (required; provided as a job parameter) ─────────────────────────
s3_bucket = _cfg("S3_BUCKET", "")
assert s3_bucket, "S3_BUCKET must be provided as a job parameter (no default)."

# ── Bronze source (REQUIRED, explicit — no /test/ default) ────────────────────
bronze_key = _cfg("BRONZE_KEY", "")
assert bronze_key, "BRONZE_KEY is required (S3 object key of the Bronze ZIP)."

# ── Silver output (REQUIRED, explicit — no /test/ default) ────────────────────
silver_s3_path = _cfg("SILVER_OUTPUT_PATH", "")
assert silver_s3_path, "SILVER_OUTPUT_PATH is required (full s3:// path)."

# ── Unity Catalog managed volume (temporary working storage) ─────────────────
uc_catalog = _cfg("UC_CATALOG", "workspace")
uc_schema  = _cfg("UC_SCHEMA",  "default")
uc_volume  = _cfg("UC_VOLUME",  "v2_temp")

# Unique identifier for this run — prevents path collisions on concurrent runs.
run_id = str(uuid.uuid4())[:8]  # short UUID for readability in logs

# Derived volume base path.
volume_base_path = f"/Volumes/{uc_catalog}/{uc_schema}/{uc_volume}"

# ── Fixture-vs-production validation ──────────────────────────────────────────
# In integration/fixture mode the caller passes EXPECTED_FRAME_ROWS; in
# production it is omitted and only invariants (row_count > 0, schema, ...) hold.
_exp_frames = _cfg("EXPECTED_FRAME_ROWS", "")
expected_frame_rows = int(_exp_frames) if _exp_frames else None
fixture_mode = expected_frame_rows is not None

print(f"run_id:          {run_id}")
print(f"validation:      {'fixture' if fixture_mode else 'production (invariants only)'}")
print(f"bronze_key:      {bronze_key}")
print(f"silver_s3_path:  {silver_s3_path}")
print(f"uc_volume:       {volume_base_path}")

# COMMAND ----------
# MAGIC %md ## 2. Create UC volume run directory and download Bronze ZIP

# COMMAND ----------

from traffic_data_elt.databricks.bronze_reader import download_and_extract, dbutils_copy_fn

# On serverless compute boto3 has no credentials.  Use dbutils.fs, which reads
# S3 through the Unity Catalog external-location credential for this bucket.
archive = download_and_extract(
    bucket=s3_bucket,
    bronze_key=bronze_key,
    volume_base_path=volume_base_path,
    run_id=run_id,
    copy_fn=dbutils_copy_fn(dbutils),  # noqa: F821 - dbutils injected by Databricks
)

print(f"run directory:       {archive.run_dir}")
print(f"ZIP downloaded to:   {archive.local_zip_path}")
print(f"CSV extracted to:    {archive.extracted_csv_path}")
print(f"ZIP member:          {archive.zip_member}")
print(f"CSV size (bytes):    {archive.extracted_csv_path.stat().st_size:,}")

# COMMAND ----------
# MAGIC %md ## 3. Parse CSV and write Silver Parquet

# COMMAND ----------

from traffic_data_elt.databricks.silver_writer import write_silver

result = write_silver(
    spark=spark,
    csv_path=archive.extracted_csv_path,
    bronze_key=bronze_key,
    silver_s3_path=silver_s3_path,
    run_id=run_id,
    coalesce_partitions=1,
)

print(f"status:               {result.status}")
print(f"run_id:               {result.run_id}")
print(f"logical vehicles:     {result.logical_vehicle_count:,}")
print(f"frame rows written:   {result.frame_row_count:,}")
print(f"rejected records:     {result.rejected_record_count:,}")
print(f"silver path:          {result.silver_path}")
print(f"elapsed:              {(result.end_time - result.start_time).total_seconds():.1f}s")

if result.status != "success":
    raise RuntimeError(
        f"Silver write failed: {result.error}\n"
        f"Temporary files retained for diagnosis at: {archive.run_dir}"
    )

# ── Invariant: parser must produce rows ───────────────────────────────────────
if result.frame_row_count <= 0:
    raise RuntimeError(
        f"Silver produced 0 frame rows — STOP.\n"
        f"Temporary files retained: {archive.run_dir}"
    )

# ── Fixture parity pre-check (integration mode only) ──────────────────────────
if fixture_mode and result.frame_row_count != expected_frame_rows:
    raise RuntimeError(
        f"FIXTURE PARITY FAILURE: expected {expected_frame_rows:,} frame rows, "
        f"got {result.frame_row_count:,} "
        f"(delta: {result.frame_row_count - expected_frame_rows:+,}).\n"
        f"Diagnose: ZIP member integrity, encoding, newline handling, "
        f"parser invocation. Temporary files retained: {archive.run_dir}"
    )

print("✓ Silver row-count checks passed")

# COMMAND ----------
# MAGIC %md ## 4. Validate Silver output (strict)
# MAGIC
# MAGIC All checks must pass before cleanup is authorised.
# MAGIC Failures halt the pipeline — temporary files are retained for diagnosis.

# COMMAND ----------

from traffic_data_elt.databricks.silver_validator import validate_silver

# expected_row_count is None in production → validator enforces invariants only.
validation = validate_silver(
    spark=spark,
    silver_path=silver_s3_path,
    expected_row_count=expected_frame_rows,
)

print(validation.summary())

if not validation.passed:
    raise RuntimeError(
        f"Silver validation FAILED — temporary files retained for diagnosis.\n"
        f"  run directory:   {archive.run_dir}\n"
        f"  extracted CSV:   {archive.extracted_csv_path}\n"
        f"  Bronze ZIP:      {archive.local_zip_path}\n"
        f"Failed checks:\n"
        + "\n".join(f"  ✗ {c}" for c in validation.failed_checks)
    )

print("✓ All Silver validation checks passed")

# COMMAND ----------
# MAGIC %md ## 5. Cleanup UC volume run directory
# MAGIC
# MAGIC Runs only after validation passes — the cell above raises on failure.

# COMMAND ----------

archive.cleanup()

print(f"✓ Temporary run directory removed: {archive.run_dir}")
print(f"  Persistent Bronze:  s3://{s3_bucket}/{bronze_key}")
print(f"  Persistent Silver:  {silver_s3_path}")

# COMMAND ----------
# MAGIC %md ## 6. Observability summary

# COMMAND ----------

elapsed_total = (result.end_time - result.start_time).total_seconds()

print("=" * 65)
print("V2 SILVER PIPELINE — COMPLETE")
print("=" * 65)
print(f"  run_id:               {run_id}")
print(f"  bronze_key:           {bronze_key}")
print(f"  zip_member:           {archive.zip_member}")
print(f"  uc_run_dir:           {archive.run_dir}")
print(f"  logical vehicles:     {result.logical_vehicle_count:,}")
print(f"  silver frame rows:    {result.frame_row_count:,}")
print(f"  rejected records:     {result.rejected_record_count:,}")
print(f"  silver path:          {result.silver_path}")
print(f"  parquet compression:  snappy")
print(f"  coalesce partitions:  1")
print(f"  parse + write time:   {elapsed_total:.1f}s")
print(f"  validation:           {'PASSED' if validation.passed else 'FAILED'}")
print(f"  cleanup:              {'done' if archive.is_cleaned_up else 'pending'}")
print("=" * 65)
