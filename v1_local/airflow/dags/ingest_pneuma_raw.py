"""ingest_pneuma_raw — DAG: load pNEUMA CSV files into raw.vehicle_trajectories.

Design
------
This DAG is intentionally thin.  All extraction and loading logic lives in
``src/traffic_data_elt/`` and is imported here; the DAG only orchestrates.

Schedule
--------
Not scheduled by default (schedule=None).  Trigger manually or set a cron
expression once files are reliably available.

Tasks
-----
1. ensure_schema   — create raw/audit tables if absent (idempotent DDL).
2. discover_files  — scan the data directory for unloaded CSV files.
3. load_file.*     — one dynamic task per file; calls RawLoader.load().
4. dbt_run         — run dbt staging+ models after successful ingestion.
5. dbt_test        — run dbt staging+ tests after successful dbt run.
6. pipeline_success — log completion after all transformations pass.

Failure handling
----------------
- default_args apply retries (2, 60 s delay) and on_failure_callback globally.
- Dependency flow uses all_success (default), so dbt tasks are skipped if
  ingestion fails, and pipeline_success is skipped if dbt fails.
- dbt exit codes are preserved — non-zero exits cause task failure.

Idempotency
-----------
RawLoader checks audit.pipeline_runs before loading; already-loaded files
are skipped.  Re-triggering the DAG is safe.

Configuration
-------------
All connection details come from environment variables injected by
compose.yaml.  No credentials appear in this file.
"""

from __future__ import annotations

import datetime
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from traffic_data_elt.airflow_callbacks import on_pipeline_success, on_task_failure
from traffic_data_elt.config import Settings
from traffic_data_elt.extract import PneumaExtractor
from traffic_data_elt.load import RawLoader
from traffic_data_elt.utils import get_logger

log = get_logger(__name__)

DAG_ID = "ingest_pneuma_raw"
DATA_DIR = os.environ.get("TRAFFIC_DATA_DIR", "/data/sample")
ROW_LIMIT = int(os.environ.get("TRAFFIC_INGEST_ROW_LIMIT", "0"))
DBT_PROJECT_DIR = "/opt/airflow/dbt/traffic_dwh"

default_args = {
    "owner": "traffic_data_elt",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=1),
    "on_failure_callback": on_task_failure,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id=DAG_ID,
    description="Load pNEUMA CSV files from the sample directory into raw.vehicle_trajectories.",
    schedule=None,
    start_date=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["v1", "ingestion", "raw", "pneuma"],
)
def ingest_pneuma_raw() -> None:

    @task(task_id="ensure_schema")
    def ensure_schema() -> None:
        """Create raw and audit tables if they do not exist."""
        settings = Settings.from_env()
        loader = RawLoader(settings, dag_id=DAG_ID, task_id="ensure_schema")
        loader.ensure_schema()

    @task(task_id="discover_files")
    def discover_files() -> list[str]:
        """Return paths of CSV files not yet successfully loaded."""
        data_path = Path(DATA_DIR)
        if not data_path.exists():
            log.warning("data directory %s does not exist — no files to load", DATA_DIR)
            return []

        candidates = sorted(data_path.glob("*.csv"))
        log.info("found %d CSV file(s) in %s", len(candidates), DATA_DIR)
        return [str(p) for p in candidates]

    @task(task_id="load_file")
    def load_file(file_path: str) -> dict[str, object]:
        """Extract and load one pNEUMA CSV file into the raw table."""
        settings = Settings.from_env()
        loader = RawLoader(
            settings,
            dag_id=DAG_ID,
            task_id="load_file",
        )
        extractor = PneumaExtractor(file_path, row_limit=ROW_LIMIT)

        log.info("starting load for %s", file_path)
        result = loader.load(file_path, extractor.extract())
        log.info("load result for %s: %s", file_path, result)
        return result

    @task(task_id="pipeline_success", on_success_callback=on_pipeline_success)
    def pipeline_success() -> None:
        """Terminal task: log that the full ELT pipeline completed."""
        log.info("All ingestion and dbt tasks completed successfully.")

    # ── Task wiring ────────────────────────────────────────────────────────────
    schema_ready = ensure_schema()
    files = discover_files()

    # Dynamic task mapping: one load_file task per discovered CSV.
    loaded = load_file.expand(file_path=files)

    # ── dbt transformation tasks ──────────────────────────────────────────────
    # The dbt project is mounted read-only. Redirect writable artifacts
    # (logs, target) to /tmp so dbt can operate without write access to
    # the project directory.
    _dbt_flags = "--log-path /tmp/dbt_logs --target-path /tmp/dbt_target"

    # Run staging and all downstream models (intermediate, marts).
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging+ {_dbt_flags}",
    )

    # Test staging and all downstream models.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select staging+ {_dbt_flags}",
    )

    # Terminal success marker.
    success = pipeline_success()

    # Dependency flow:
    # ensure_schema -> discover_files -> load_file[*] -> dbt_run -> dbt_test -> pipeline_success
    schema_ready >> files >> loaded >> dbt_run >> dbt_test >> success


ingest_pneuma_raw()
