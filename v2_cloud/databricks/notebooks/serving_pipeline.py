# Databricks notebook source
# MAGIC %md
# MAGIC # V2 Serving Pipeline — S3 Gold Parquet → Neon serving table
# MAGIC
# MAGIC **Execution model:** Databricks Serverless Compute
# MAGIC
# MAGIC **Flow:**
# MAGIC ```
# MAGIC S3 Gold trajectory_summary Parquet  (GOLD_INPUT_PATH)
# MAGIC     ↓  spark.read.parquet  (UC external location — no boto3, no keys)
# MAGIC     ↓  load_gold_to_neon(...)  — bounded COPY over TLS
# MAGIC     ↓  serving.<table>__staging_<run_id>  (staged + validated)
# MAGIC     ↓  publish per LOAD_MODE (replace_sources default | replace_snapshot)
# MAGIC     ↓  serving.gold_trajectory_summary   (Neon)
# MAGIC     ↓  measure real PostgreSQL storage
# MAGIC ```
# MAGIC
# MAGIC **Configuration-driven.** No hardcoded S3 path, Neon endpoint, secret
# MAGIC scope/key, or fixture row counts. The runtime is parameterised via job
# MAGIC parameters (widgets) / environment variables. The Neon password comes
# MAGIC from a configurable Databricks secret scope/key.
# MAGIC
# MAGIC **Runtime imports come from the installed `traffic_data_elt` wheel** — no
# MAGIC UC-volume source sync / `sys.path` workaround.

# COMMAND ----------
# MAGIC %md ## 0. Install the versioned wheel + psycopg (v3)
# MAGIC
# MAGIC `WHEEL_PATH` points at the deployed artifact (see
# MAGIC `scripts/deploy_databricks_artifact.py`). The serverless runtime ships
# MAGIC psycopg2; the loader uses psycopg v3, installed here.

# COMMAND ----------

# WHEEL_PATH is provided as a job parameter; falls back to the conventional
# artifact volume location for interactive runs.
dbutils.widgets.text("WHEEL_PATH", "")
_wheel = dbutils.widgets.get("WHEEL_PATH") or \
    "/Volumes/workspace/default/v2_artifacts/wheels/traffic_data_elt-latest-py3-none-any.whl"
%pip install --no-deps {_wheel}
%pip install --quiet "psycopg[binary]>=3.1,<4"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ## 1. Configuration (job parameters / environment)

# COMMAND ----------

import os
import uuid


def _cfg(name: str, default: str = "") -> str:
    """Read config from a notebook widget, then env var, then default."""
    try:
        v = dbutils.widgets.get(name)  # noqa: F821
        if v:
            return v
    except Exception:
        pass
    return os.environ.get(name, default)


# ── Gold input (REQUIRED, explicit — no /test/ default) ───────────────────────
# Full s3://.../<gold path> to the Snappy Parquet Gold trajectory_summary.
gold_input_path = _cfg("GOLD_INPUT_PATH", "")
assert gold_input_path, (
    "GOLD_INPUT_PATH is required (full s3:// path to the Gold Parquet). "
    "Runtime must not assume a /test/ fixture prefix."
)
assert gold_input_path.startswith("s3://") or gold_input_path.startswith("dbfs:"), (
    f"GOLD_INPUT_PATH must be an s3:// or dbfs: path, got: {gold_input_path[:12]}..."
)

# ── Environment identifier (Neon branch / logical env) ────────────────────────
neon_branch = _cfg("NEON_BRANCH", "")  # e.g. dev, staging (informational + guard)

# ── Load behaviour ────────────────────────────────────────────────────────────
load_mode = _cfg("LOAD_MODE", "replace_sources")   # default production-safe
copy_batch_size = int(_cfg("NEON_COPY_BATCH_SIZE", "10000"))

# ── Validation mode: fixture (integration) vs production ──────────────────────
# In fixture mode the caller passes the expected counts as parameters; in
# production mode they are omitted and only data invariants are enforced.
_exp_rows = _cfg("EXPECTED_ROW_COUNT", "")
_exp_frames = _cfg("EXPECTED_FRAME_SUM", "")
expected_row_count = int(_exp_rows) if _exp_rows else None
expected_frame_sum = int(_exp_frames) if _exp_frames else None
fixture_mode = expected_row_count is not None or expected_frame_sum is not None

# ── Neon connection (non-secret via params; password via secret scope) ────────
neon_host = _cfg("NEON_DB_HOST", "")
neon_port = int(_cfg("NEON_DB_PORT", "5432"))
neon_db = _cfg("NEON_DB_NAME", "")
neon_user = _cfg("NEON_DB_USER", "")
neon_sslmode = _cfg("NEON_DB_SSLMODE", "require")
assert neon_host and neon_db and neon_user, "NEON_DB_HOST/NAME/USER are required."

# ── Neon password from a CONFIGURABLE Databricks secret scope/key ─────────────
neon_secret_scope = _cfg("NEON_SECRET_SCOPE", "v2-neon")
neon_secret_key = _cfg("NEON_SECRET_KEY", "db-password")
neon_password = dbutils.secrets.get(scope=neon_secret_scope, key=neon_secret_key)

# ── Optional temp-volume teardown (opt-in) ────────────────────────────────────
# The serving job does not itself use the Silver working volume (v2_temp), but
# it can drop it after a successful Neon load to leave a clean slate for a
# from-scratch end-to-end rebuild. Default OFF so a normal run never destroys a
# volume the Silver job depends on. Enable with DROP_TEMP_VOLUME_ON_SUCCESS=true.
drop_temp_volume = _cfg("DROP_TEMP_VOLUME_ON_SUCCESS", "false").lower() == "true"
temp_uc_catalog = _cfg("UC_CATALOG", "workspace")
temp_uc_schema = _cfg("UC_SCHEMA", "default")
temp_uc_volume = _cfg("UC_VOLUME", "v2_temp")

# ── Production protection: configuration/environment based (NOT host substring)
# The deployment preflight (scripts/validate_neon_target.py) confirms the
# configured DB endpoint belongs to the configured Neon branch via the control
# plane before the job is submitted. This notebook additionally requires an
# explicit acknowledgement to publish to a production environment, so a stray
# production target cannot be written by accident.
allow_production = _cfg("ALLOW_PRODUCTION_WRITE", "false").lower() == "true"
if neon_branch.lower() in ("production", "prod") and not allow_production:
    raise RuntimeError(
        f"Refusing to write to Neon branch '{neon_branch}' without "
        f"ALLOW_PRODUCTION_WRITE=true. Production promotion is a separate, "
        f"explicitly-authorised milestone."
    )

run_id = "r" + uuid.uuid4().hex[:8]  # identifier-safe (letters+digits)

print(f"run_id:          {run_id}")
print(f"gold_input_path: {gold_input_path}")
print(f"neon_branch:     {neon_branch or '(unset)'}")
print(f"load_mode:       {load_mode}")
print(f"copy_batch_size: {copy_batch_size}")
print(f"validation:      {'fixture' if fixture_mode else 'production (invariants only)'}")
print(f"secret:          scope={neon_secret_scope} key={neon_secret_key}")

# COMMAND ----------
# MAGIC %md ## 2. Read Gold Parquet from S3

# COMMAND ----------

gold_df = spark.read.parquet(gold_input_path)
gold_count = gold_df.count()
print(f"gold rows read: {gold_count:,}")

# Production invariant: input must be non-empty. Fixture mode additionally
# checks the exact expected row count.
assert gold_count > 0, "Gold input is empty — STOP."
if fixture_mode and expected_row_count is not None:
    assert gold_count == expected_row_count, (
        f"fixture expected {expected_row_count} Gold rows, got {gold_count}"
    )

# COMMAND ----------
# MAGIC %md ## 3. Load Gold → Neon (stage → validate → publish per load mode)

# COMMAND ----------

from traffic_data_elt.databricks.neon_loader import load_gold_to_neon

conninfo = {
    "host": neon_host,
    "port": neon_port,
    "dbname": neon_db,
    "user": neon_user,
    "password": neon_password,   # from secret scope; never printed
    "sslmode": neon_sslmode,
}

result = load_gold_to_neon(
    gold_df,
    conninfo,
    run_id=run_id,
    load_mode=load_mode,
    copy_batch_size=copy_batch_size,
    expected_row_count=expected_row_count,   # None in production → invariants only
    expected_frame_sum=expected_frame_sum,
)

print(f"status:              {result.status}")
print(f"load_mode:           {result.load_mode}")
print(f"serving table:       {result.serving_table}")
print(f"source files:        {result.source_files}")
print(f"staged rows:         {result.staged_row_count:,}")
print(f"published rows:      {result.published_row_count:,}")
print(f"sum(frame_count):    {result.sum_frame_count:,}")
print(f"distinct grain:      {result.distinct_grain:,}")
print(f"validation passed:   {result.validation_passed}")
print(f"staging cleaned:     {result.staging_cleaned}")

assert result.status == "success", f"Neon load failed: {result.error}"
if fixture_mode and expected_frame_sum is not None:
    assert result.frames_conserved(expected_frame_sum), "frame conservation FAILED"
print("✓ Gold → Neon serving load complete")

# COMMAND ----------
# MAGIC %md ## 3b. (Opt-in) Drop the temporary Silver working volume
# MAGIC
# MAGIC Only runs after a **successful** Neon load and only when
# MAGIC `DROP_TEMP_VOLUME_ON_SUCCESS=true`. Intended for a clean-slate
# MAGIC end-to-end rebuild — it removes the whole `v2_temp` UC volume, which the
# MAGIC Silver job would then need recreated (see
# MAGIC `v2_cloud/databricks/setup/create_uc_volume.sql`). The persistent wheel
# MAGIC artifact volume (`v2_artifacts`) is never touched here.

# COMMAND ----------

if drop_temp_volume:
    # Reusable packaged teardown — identical call an Airflow DAG would make.
    from traffic_data_elt.databricks.bronze_reader import drop_temp_volume as _drop_temp_volume

    _temp_fqn = _drop_temp_volume(
        spark,
        catalog=temp_uc_catalog,
        schema=temp_uc_schema,
        volume=temp_uc_volume,
    )
    print(f"✓ dropped temporary volume {_temp_fqn} (recreate before next Silver run)")
else:
    print("temp-volume teardown skipped (DROP_TEMP_VOLUME_ON_SUCCESS not set)")

# COMMAND ----------
# MAGIC %md ## 4. Measure real Neon storage

# COMMAND ----------

from traffic_data_elt.databricks.neon_loader import (
    bytes_per_trajectory,
    measure_storage,
    project_full_storage,
)

storage = measure_storage(conninfo)
bpt = bytes_per_trajectory(storage["total_bytes"], result.published_row_count)
proj_500k = project_full_storage(storage["total_bytes"], result.published_row_count, 500_000)


def _mb(n: int) -> str:
    return f"{n / 1024 / 1024:.2f} MiB"


print("=" * 60)
print("NEON SERVING STORAGE (measured)")
print("=" * 60)
print(f"  table_bytes:       {storage['table_bytes']:,} ({_mb(storage['table_bytes'])})")
print(f"  index_bytes:       {storage['index_bytes']:,} ({_mb(storage['index_bytes'])})")
print(f"  total_bytes:       {storage['total_bytes']:,} ({_mb(storage['total_bytes'])})")
print(f"  database_bytes:    {storage['database_bytes']:,} ({_mb(storage['database_bytes'])})")
print(f"  bytes/trajectory:  {bpt:,.1f}")
print("-" * 60)
print("  PROJECTION @ 500,000 trajectories (linear from current data):")
print(f"    ~{proj_500k:,} bytes  ({_mb(proj_500k)})")
print("    design target: <= 300–350 MB total serving footprint")
print("=" * 60)

# COMMAND ----------
# MAGIC %md ## 5. Observability summary

# COMMAND ----------

print("V2 SERVING PIPELINE — COMPLETE")
print(f"  run_id:            {run_id}")
print(f"  neon_branch:       {neon_branch or '(unset)'}")
print(f"  gold input:        {gold_input_path}")
print(f"  load_mode:         {result.load_mode}")
print(f"  serving table:     {result.serving_table}")
print(f"  published rows:    {result.published_row_count:,}")
print(f"  sum(frame_count):  {result.sum_frame_count:,}")
print(f"  distinct grain:    {result.distinct_grain:,}")
print(f"  source files:      {result.source_files}")
print(f"  total relation:    {_mb(storage['total_bytes'])}")
print(f"  start:             {result.start_time.isoformat()}")
print(f"  end:               {result.end_time.isoformat()}")
