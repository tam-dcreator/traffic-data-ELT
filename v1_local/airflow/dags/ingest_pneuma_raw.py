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

from traffic_data_elt.config import Settings
from traffic_data_elt.extract import PneumaExtractor
from traffic_data_elt.load import RawLoader
from traffic_data_elt.utils import get_logger

log = get_logger(__name__)

DAG_ID = "ingest_pneuma_raw"
DATA_DIR = os.environ.get("TRAFFIC_DATA_DIR", "/data/sample")
ROW_LIMIT = int(os.environ.get("TRAFFIC_INGEST_ROW_LIMIT", "0"))


@dag(
    dag_id=DAG_ID,
    description="Load pNEUMA CSV files from the sample directory into raw.vehicle_trajectories.",
    schedule=None,
    start_date=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    max_active_runs=1,
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
        # Pass the full path so RawLoader can compute the file hash for
        # idempotency.  source_file (basename) is derived inside the loader.
        result = loader.load(file_path, extractor.extract())
        log.info("load result for %s: %s", file_path, result)
        return result

    # ── Task wiring ────────────────────────────────────────────────────────────
    schema_ready = ensure_schema()
    files = discover_files()

    # Dynamic task mapping: one load_file task per discovered CSV.
    # expand() creates tasks at runtime; the DAG graph is still static.
    loaded = load_file.expand(file_path=files)

    schema_ready >> files >> loaded


ingest_pneuma_raw()
