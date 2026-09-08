"""V2 production Airflow DAG — full pNEUMA archive, end to end.

    ZENODO_URL → S3 Bronze → Databricks Silver → S3 Silver
      → Databricks Gold → S3 Gold → Neon production → dbt v2_production
      → production validation → drop v2_temp → DAG SUCCESS

Orchestration ONLY. Every task delegates to already-tested, packaged reusable
logic (``traffic_data_elt.databricks.*``, ``traffic_data_elt.config``,
``traffic_data_elt.extract``/``load``) or shells to the pinned CLI tools. No
parser/Silver/Gold/loader/dbt logic lives in this file. XCom carries only small
metadata (paths, run IDs, counts, wheel identity, storage metrics) — never data.

Safety
------
* ``catchup=False`` and ``max_active_runs=1`` (mandatory: the shared ``v2_temp``
  volume is dropped after a successful run; two concurrent runs must never
  share then delete it).
* Production writes require the Neon control-plane preflight AND
  ``ALLOW_PRODUCTION_WRITE=true``.
* The final ``drop_v2_temp_volume`` runs only on ``ALL_SUCCESS`` of every prior
  production gate; a failed run retains ``v2_temp`` for debugging.
* Manual trigger only (``schedule=None``); no recurring production schedule yet.
"""

from __future__ import annotations

import datetime
import os
import sys
from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

# ── Make the shared package importable (mounted at /opt/airflow/src) ──────────
_SRC_CANDIDATES = ("/opt/airflow/src", str(Path(__file__).resolve().parents[3] / "src"))
for _p in _SRC_CANDIDATES:
    if _p not in sys.path and os.path.isdir(_p):
        sys.path.insert(0, _p)

from traffic_data_elt.airflow_callbacks import on_pipeline_success, on_task_failure  # noqa: E402
from traffic_data_elt.config import AwsConfig, NeonConfig  # noqa: E402
from traffic_data_elt.databricks import bootstrap, production_pipeline as pp  # noqa: E402
from traffic_data_elt.utils import get_logger  # noqa: E402

log = get_logger(__name__)

DAG_ID = "traffic_data_v2_production"

# ── Non-secret runtime configuration (env-driven; no hardcoded identifiers) ───
DATABRICKS_PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/traffic_dwh")
REPO_ROOT = os.environ.get("REPO_ROOT", "/opt/airflow")

UC_CATALOG = os.environ.get("UC_CATALOG", "workspace")
UC_SCHEMA = os.environ.get("UC_SCHEMA", "default")
UC_VOLUME = os.environ.get("UC_VOLUME", "v2_temp")
ARTIFACT_VOLUME = os.environ.get("ARTIFACT_VOLUME", "v2_artifacts")
ARTIFACT_PATH = os.environ.get(
    "ARTIFACT_PATH", f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{ARTIFACT_VOLUME}/wheels"
)

NEON_SECRET_SCOPE = os.environ.get("NEON_SECRET_SCOPE", "v2-neon")
NEON_SECRET_KEY = os.environ.get("NEON_SECRET_KEY", "db-password")
NEON_COPY_BATCH_SIZE = os.environ.get("NEON_COPY_BATCH_SIZE", "10000")

# Notebook workspace directory + local source dir (deployed by a bootstrap task).
NOTEBOOK_WORKSPACE_DIR = os.environ.get("NOTEBOOK_WORKSPACE_DIR", "/traffic_data")
NOTEBOOKS_DIR = os.environ.get(
    "NOTEBOOKS_DIR", os.path.join(REPO_ROOT, "v2_cloud", "databricks", "notebooks")
)
SILVER_NOTEBOOK = os.environ.get("SILVER_NOTEBOOK", f"{NOTEBOOK_WORKSPACE_DIR}/silver_pipeline")
GOLD_NOTEBOOK = os.environ.get("GOLD_NOTEBOOK", f"{NOTEBOOK_WORKSPACE_DIR}/gold_pipeline")
SERVING_NOTEBOOK = os.environ.get("SERVING_NOTEBOOK", f"{NOTEBOOK_WORKSPACE_DIR}/serving_pipeline")

# Data-quality gate: max fraction of rejected records tolerated in Silver.
SILVER_MAX_REJECT_FRACTION = float(os.environ.get("SILVER_MAX_REJECT_FRACTION", "0.01"))


default_args = {
    "owner": "traffic_data_elt",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=2),
    "on_failure_callback": on_task_failure,
    "email_on_failure": False,
    "email_on_retry": False,
}


def _poll_databricks_run(run_id: str, *, timeout_min: int = 240) -> None:
    """Block until a Databricks run terminates; raise on non-success."""
    import time

    deadline = time.time() + timeout_min * 60
    while time.time() < deadline:
        life, result = pp.get_run_state(DATABRICKS_PROFILE, run_id)
        if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            if result != "SUCCESS":
                raise AirflowException(
                    f"Databricks run {run_id} finished {life}/{result}"
                )
            return
        time.sleep(30)
    raise AirflowException(f"Databricks run {run_id} timed out after {timeout_min}m")


@dag(
    dag_id=DAG_ID,
    description="V2 production: full pNEUMA archive → S3 → Databricks → Neon → dbt.",
    schedule=None,               # manual trigger only for the first full run
    start_date=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    max_active_runs=1,           # mandatory: shared v2_temp is dropped after success
    default_args=default_args,
    tags=["v2", "production", "pneuma", "databricks", "neon"],
)
def traffic_data_v2_production() -> None:

    # ── Preflight ─────────────────────────────────────────────────────────────
    @task(task_id="preflight_config")
    def preflight_config() -> dict:
        """Validate required non-secret config; resolve + report paths."""
        aws = AwsConfig.from_env()
        if not aws.zenodo_url:
            raise AirflowException("ZENODO_URL is required for production ingestion.")
        paths = pp.resolve_paths(aws)
        for name, value in paths.as_dict().items():
            if "/test/" in str(value) or str(value).endswith("/test"):
                raise AirflowException(f"production path {name} must not use /test/: {value}")
        wheel_name = bootstrap.versioned_wheel_name(
            _project_version(), bootstrap.resolve_git_sha(REPO_ROOT)
        )
        report = {
            **paths.as_dict(),
            "artifact_path": ARTIFACT_PATH,
            "temp_volume": f"{UC_CATALOG}.{UC_SCHEMA}.{UC_VOLUME}",
            "wheel_name": wheel_name,
            "neon_branch": os.environ.get("NEON_BRANCH", ""),
            "dbt_target": _dbt_target(),
        }
        log.info("preflight_config resolved: %s", report)
        return report

    @task(task_id="preflight_databricks")
    def preflight_databricks(cfg: dict) -> dict:
        """Confirm Databricks CLI auth is usable (no secrets printed)."""
        res = bootstrap.default_command_runner(
            bootstrap._databricks_argv(DATABRICKS_PROFILE, "current-user", "me")
        )
        if not res.ok:
            raise AirflowException(f"Databricks auth check failed: {res.stderr.strip()}")
        return cfg

    @task(task_id="preflight_neon_production")
    def preflight_neon_production(cfg: dict) -> dict:
        """Neon control-plane preflight + production write acknowledgement."""
        branch = os.environ.get("NEON_BRANCH", "")
        allow = os.environ.get("ALLOW_PRODUCTION_WRITE", "false").lower() == "true"
        if branch.lower() in ("production", "prod") and not allow:
            raise AirflowException(
                "Refusing production write without ALLOW_PRODUCTION_WRITE=true."
            )
        # Reuse the packaged control-plane preflight script (API key via env only).
        res = bootstrap.default_command_runner(
            [sys.executable, os.path.join(REPO_ROOT, "scripts", "validate_neon_target.py"),
             "--env-file", os.environ.get("V2_ENV_FILE", "v2_cloud/.env")]
        )
        if not res.ok:
            raise AirflowException(f"Neon control-plane preflight failed: {res.stderr.strip()}")
        log.info("Neon production preflight OK for branch=%s", branch)
        return cfg

    # ── Bootstrap Databricks runtime ────────────────────────────────────────────
    @task_group(group_id="bootstrap_databricks_runtime")
    def bootstrap_databricks_runtime(cfg: dict) -> dict:
        @task(task_id="ensure_artifact_volume")
        def ensure_artifact_volume(cfg: dict) -> dict:
            fqn = bootstrap.ensure_artifact_volume(DATABRICKS_PROFILE, ARTIFACT_PATH)
            return {**cfg, "artifact_volume_fqn": fqn}

        @task(task_id="ensure_versioned_wheel")
        def ensure_versioned_wheel(cfg: dict) -> dict:
            result = bootstrap.ensure_versioned_wheel(
                DATABRICKS_PROFILE, ARTIFACT_PATH,
                repo_root=REPO_ROOT, version=_project_version(),
            )
            return {**cfg, "wheel_path": result.wheel_path,
                    "wheel_name": result.wheel_name, "wheel_reused": result.reused,
                    "git_sha": result.git_sha}

        @task(task_id="ensure_databricks_notebooks")
        def ensure_databricks_notebooks(cfg: dict) -> dict:
            results = bootstrap.ensure_databricks_notebooks(
                DATABRICKS_PROFILE, NOTEBOOKS_DIR,
                workspace_dir=NOTEBOOK_WORKSPACE_DIR,
            )
            return {**cfg, "notebooks": {r.name: r.action for r in results}}

        @task(task_id="ensure_v2_temp")
        def ensure_v2_temp(cfg: dict) -> dict:
            fqn = bootstrap.ensure_temp_volume(
                DATABRICKS_PROFILE, f"{UC_CATALOG}.{UC_SCHEMA}.{UC_VOLUME}"
            )
            return {**cfg, "temp_volume_fqn": fqn}

        # Order: artifact volume → versioned wheel → notebooks → v2_temp.
        return ensure_v2_temp(
            ensure_databricks_notebooks(
                ensure_versioned_wheel(ensure_artifact_volume(cfg))
            )
        )

    # ── Bronze ────────────────────────────────────────────────────────────────
    @task(task_id="ingest_full_bronze")
    def ingest_full_bronze(cfg: dict) -> dict:
        """Stream the archive from ZENODO_URL → S3 Bronze (multipart, idempotent)."""
        from traffic_data_elt.extract import ZenodoStreamExtractor
        from traffic_data_elt.load import S3Uploader

        aws = AwsConfig.from_env()
        extractor = ZenodoStreamExtractor(aws.zenodo_url, chunk_bytes=aws.http_chunk_bytes)
        expected_len = extractor.content_length
        state = pp.inspect_bronze_object(
            cfg["bucket"], cfg["bronze_key"], region=aws.region,
            expected_length=expected_len,
        )
        if state.reuse:
            log.info("Bronze reuse: %s (%s bytes) — %s",
                     cfg["bronze_key"], state.size_bytes, state.reason)
            return {**cfg, "bronze_bytes": state.size_bytes, "bronze_decision": "reused"}

        uploader = S3Uploader(aws)
        with extractor.open() as body:
            result = uploader.upload_stream(body, cfg["bronze_object_name"])
        log.info("Bronze upload complete: %s (%s bytes)", result.key, result.bytes_transferred)
        return {**cfg, "bronze_bytes": result.bytes_transferred, "bronze_decision": "uploaded"}

    @task(task_id="validate_bronze")
    def validate_bronze(cfg: dict) -> dict:
        aws = AwsConfig.from_env()
        state = pp.inspect_bronze_object(cfg["bucket"], cfg["bronze_key"], region=aws.region)
        if not state.exists or not state.size_bytes:
            raise AirflowException(f"Bronze object invalid: {cfg['bronze_key']} ({state.reason})")
        return cfg

    # ── Silver ──────────────────────────────────────────────────────────────────
    @task(task_id="run_full_silver")
    def run_full_silver(cfg: dict) -> dict:
        params = pp.silver_job_params(
            cfg["wheel_path"], cfg["bucket"], cfg["bronze_key"], cfg["silver_root"],
            uc_catalog=UC_CATALOG, uc_schema=UC_SCHEMA, uc_volume=UC_VOLUME,
        )
        run_id = pp.submit_notebook_job(
            DATABRICKS_PROFILE, "v2-prod-silver", SILVER_NOTEBOOK, params
        )
        _poll_databricks_run(run_id)
        return {**cfg, "silver_run_id": run_id}

    @task(task_id="validate_silver")
    def validate_silver(cfg: dict) -> dict:
        """Dynamic Silver validation (invariants only; no fixture counts)."""
        aws = AwsConfig.from_env()
        metrics = _read_parquet_metrics(aws, cfg["silver_root"])
        if metrics["row_count"] <= 0:
            raise AirflowException("Silver row_count must be > 0.")
        if metrics["source_file_count"] <= 0:
            raise AirflowException("Silver must contain at least one source_file.")
        return {**cfg, "silver_rows": metrics["row_count"],
                "silver_source_files": metrics["source_file_count"]}

    # ── Gold ────────────────────────────────────────────────────────────────────
    @task(task_id="run_full_gold")
    def run_full_gold(cfg: dict) -> dict:
        params = pp.gold_job_params(cfg["wheel_path"], cfg["silver_root"], cfg["gold_root"])
        run_id = pp.submit_notebook_job(
            DATABRICKS_PROFILE, "v2-prod-gold", GOLD_NOTEBOOK, params
        )
        _poll_databricks_run(run_id)
        return {**cfg, "gold_run_id": run_id}

    @task(task_id="validate_gold")
    def validate_gold(cfg: dict) -> dict:
        """Dynamic Gold validation incl. frame conservation vs Silver."""
        aws = AwsConfig.from_env()
        metrics = _read_gold_metrics(aws, cfg["gold_root"])
        if metrics["row_count"] <= 0:
            raise AirflowException("Gold row_count must be > 0.")
        if metrics["frame_sum"] != cfg["silver_rows"]:
            raise AirflowException(
                f"frame conservation FAILED: Gold SUM(frame_count)={metrics['frame_sum']} "
                f"!= Silver rows={cfg['silver_rows']}"
            )
        return {**cfg, "gold_rows": metrics["row_count"], "gold_frame_sum": metrics["frame_sum"]}

    # ── Neon production ───────────────────────────────────────────────────────────
    @task(task_id="load_neon_production")
    def load_neon_production(cfg: dict) -> dict:
        neon = NeonConfig.from_env()
        allow = os.environ.get("ALLOW_PRODUCTION_WRITE", "false").lower() == "true"
        params = pp.serving_job_params(
            cfg["wheel_path"], cfg["gold_root"],
            neon_host=neon.host, neon_port=str(neon.port), neon_db=neon.database,
            neon_user=neon.user, neon_sslmode=neon.sslmode,
            neon_secret_scope=NEON_SECRET_SCOPE, neon_secret_key=NEON_SECRET_KEY,
            neon_branch=os.environ.get("NEON_BRANCH", ""),
            load_mode="replace_sources", copy_batch_size=NEON_COPY_BATCH_SIZE,
            allow_production_write=allow,
        )
        run_id = pp.submit_notebook_job(
            DATABRICKS_PROFILE, "v2-prod-serving", SERVING_NOTEBOOK, params
        )
        _poll_databricks_run(run_id)
        return {**cfg, "serving_run_id": run_id}

    @task(task_id="validate_neon_production")
    def validate_neon_production(cfg: dict) -> dict:
        neon = NeonConfig.from_env()
        m = _read_neon_metrics(neon)
        v = pp.evaluate_neon_production(
            neon_row_count=m["row_count"], neon_frame_sum=m["frame_sum"],
            distinct_grain=m["distinct_grain"],
            gold_row_count=cfg["gold_rows"], gold_frame_sum=cfg["gold_frame_sum"],
        )
        if not v.passed:
            raise AirflowException(f"Neon production validation failed: {v.failed_checks}")
        return {**cfg, "neon_rows": m["row_count"], "neon_frame_sum": m["frame_sum"],
                "neon_total_bytes": m.get("total_bytes")}

    # ── dbt ───────────────────────────────────────────────────────────────────────
    @task(task_id="run_dbt_v2_production")
    def run_dbt_v2_production(cfg: dict) -> dict:
        target = _dbt_target()
        flags = "--log-path /tmp/dbt_logs --target-path /tmp/dbt_target"
        run = bootstrap.default_command_runner(
            ["bash", "-lc",
             f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --target {target} "
             f"--select fct_vehicle_trajectories dim_vehicle_type {flags}"]
        )
        if not run.ok:
            raise AirflowException(f"dbt run failed: {run.stderr.strip() or run.stdout.strip()}")
        test = bootstrap.default_command_runner(
            ["bash", "-lc",
             f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir . --target {target} "
             f"--select fct_vehicle_trajectories dim_vehicle_type {flags}"]
        )
        if not test.ok:
            raise AirflowException(f"dbt test failed: {test.stderr.strip() or test.stdout.strip()}")
        return {**cfg, "dbt_target": target}

    @task(task_id="validate_dbt_production")
    def validate_dbt_production(cfg: dict) -> dict:
        neon = NeonConfig.from_env()
        fct = _read_scalar(neon, "select count(*) from marts.fct_vehicle_trajectories")
        if fct != cfg["neon_rows"]:
            raise AirflowException(
                f"dbt fact rows {fct} != Neon serving rows {cfg['neon_rows']}"
            )
        return cfg

    # ── Cleanup (ALL_SUCCESS only) ──────────────────────────────────────────────
    @task(task_id="drop_v2_temp_volume", trigger_rule=TriggerRule.ALL_SUCCESS)
    def drop_v2_temp_volume(cfg: dict) -> dict:
        res = bootstrap.default_command_runner(
            bootstrap._databricks_argv(
                DATABRICKS_PROFILE, "volumes", "delete",
                f"{UC_CATALOG}.{UC_SCHEMA}.{UC_VOLUME}"
            )
        )
        # Idempotent: a missing volume is acceptable (already gone).
        if not res.ok and "does not exist" not in (res.stderr or "").lower():
            raise AirflowException(f"drop v2_temp failed: {res.stderr.strip()}")
        return cfg

    @task(task_id="verify_v2_temp_removed", trigger_rule=TriggerRule.ALL_SUCCESS)
    def verify_v2_temp_removed(cfg: dict) -> dict:
        res = bootstrap.default_command_runner(
            bootstrap._databricks_argv(
                DATABRICKS_PROFILE, "volumes", "read",
                f"{UC_CATALOG}.{UC_SCHEMA}.{UC_VOLUME}"
            )
        )
        if res.ok:
            raise AirflowException("v2_temp still exists after cleanup — failing DAG.")
        return cfg

    @task(task_id="pipeline_success", on_success_callback=on_pipeline_success,
          trigger_rule=TriggerRule.ALL_SUCCESS)
    def pipeline_success(cfg: dict) -> None:
        log.info("V2 production pipeline complete. Summary: %s", cfg)

    # ── Wiring ──────────────────────────────────────────────────────────────────
    c0 = preflight_config()
    c1 = preflight_databricks(c0)
    c2 = preflight_neon_production(c1)
    boot = bootstrap_databricks_runtime(c2)
    b1 = ingest_full_bronze(boot)
    b2 = validate_bronze(b1)
    s1 = run_full_silver(b2)
    s2 = validate_silver(s1)
    g1 = run_full_gold(s2)
    g2 = validate_gold(g1)
    n1 = load_neon_production(g2)
    n2 = validate_neon_production(n1)
    d1 = run_dbt_v2_production(n2)
    d2 = validate_dbt_production(d1)
    drop = drop_v2_temp_volume(d2)
    verify = verify_v2_temp_removed(drop)
    pipeline_success(verify)


# ---------------------------------------------------------------------------
# Module-level helpers (import-safe; used by tasks above)
# ---------------------------------------------------------------------------


def _project_version() -> str:
    """Read the project version from the installed package metadata."""
    try:
        from importlib.metadata import version
        return version("traffic-data-elt")
    except Exception:
        return os.environ.get("PROJECT_VERSION", "0.1.0")


def _dbt_target() -> str:
    """Derive the dbt target from NEON_BRANCH (v2_<branch>)."""
    branch = os.environ.get("NEON_BRANCH", "production")
    return f"v2_{branch}"


def _read_parquet_metrics(aws, silver_path: str) -> dict:
    """Read Silver row_count + distinct source_file count via boto3+pyarrow.

    Isolated here so the DAG body stays thin; not exercised by unit tests
    (requires live S3 + parquet libs).
    """
    import pyarrow.dataset as ds  # noqa: PLC0415
    dataset = ds.dataset(silver_path, format="parquet")
    table = dataset.to_table(columns=["source_file"])
    source_files = set(table.column("source_file").to_pylist())
    return {"row_count": dataset.count_rows(), "source_file_count": len(source_files)}


def _read_gold_metrics(aws, gold_path: str) -> dict:
    """Read Gold row_count + SUM(frame_count) via pyarrow."""
    import pyarrow.compute as pc  # noqa: PLC0415
    import pyarrow.dataset as ds  # noqa: PLC0415
    dataset = ds.dataset(gold_path, format="parquet")
    table = dataset.to_table(columns=["frame_count"])
    frame_sum = int(pc.sum(table.column("frame_count")).as_py() or 0)
    return {"row_count": dataset.count_rows(), "frame_sum": frame_sum}


def _read_neon_metrics(neon) -> dict:
    """Read Neon serving row_count, SUM(frame_count), distinct grain, sizes."""
    import psycopg  # noqa: PLC0415
    from traffic_data_elt.databricks.schemas import serving_schema as ss  # noqa: PLC0415

    fq = ss.qualified_serving_table()
    with psycopg.connect(connect_timeout=30, **neon.conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"select count(*), coalesce(sum(frame_count),0), "
                f"count(distinct (source_file, track_id)) from {fq}"
            )
            rows, frames, grain = cur.fetchone()
            cur.execute(f"select pg_total_relation_size('{fq}')")
            total_bytes = cur.fetchone()[0]
    return {"row_count": int(rows), "frame_sum": int(frames),
            "distinct_grain": int(grain), "total_bytes": int(total_bytes)}


def _read_scalar(neon, sql: str) -> int:
    import psycopg  # noqa: PLC0415
    with psycopg.connect(connect_timeout=30, **neon.conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return int(cur.fetchone()[0])


traffic_data_v2_production()
