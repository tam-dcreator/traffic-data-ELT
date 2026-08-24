"""RawLoader — loads pNEUMA frame records into raw.vehicle_trajectories.

Design decisions
----------------
- Uses ``psycopg`` (v3) with ``executemany`` and ``COPY`` semantics via
  ``copy`` helpers for efficient bulk inserts.
- Idempotent: a (source_file) uniqueness guard prevents re-loading a file
  that was already successfully loaded.  The check is done at the start of
  each load operation; a partially-loaded file is cleaned up and retried.
- All connection credentials come from :class:`~traffic_data_elt.config.Settings`;
  they are never logged.
- Audit metadata is written to ``audit.pipeline_runs`` on success or failure
  so every load attempt is observable.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict
from typing import Iterator

import psycopg
import psycopg.rows

from traffic_data_elt.config import Settings
from traffic_data_elt.extract.pneuma import PneumaRecord
from traffic_data_elt.utils import get_logger

log = get_logger(__name__)

# DDL run once at startup to ensure destination tables exist.
_DDL_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS raw.vehicle_trajectories (
    id                BIGSERIAL PRIMARY KEY,
    source_file       TEXT        NOT NULL,
    track_id          INTEGER     NOT NULL,
    vehicle_type      TEXT        NOT NULL,
    traveled_d_m      DOUBLE PRECISION NOT NULL,
    avg_speed_ms      DOUBLE PRECISION NOT NULL,
    lat               DOUBLE PRECISION NOT NULL,
    lon               DOUBLE PRECISION NOT NULL,
    speed_ms          DOUBLE PRECISION NOT NULL,
    lon_acc_ms2       DOUBLE PRECISION NOT NULL,
    lat_acc_ms2       DOUBLE PRECISION NOT NULL,
    timestamp_s       DOUBLE PRECISION NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

_DDL_RAW_INDEX = """
CREATE INDEX IF NOT EXISTS ix_raw_vt_source_file
    ON raw.vehicle_trajectories (source_file);
"""

_DDL_AUDIT_TABLE = """
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id            UUID        PRIMARY KEY,
    dag_id            TEXT,
    task_id           TEXT,
    source_file       TEXT,
    status            TEXT        NOT NULL,
    rows_loaded       BIGINT,
    rows_rejected     BIGINT,
    error_message     TEXT,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    duration_s        DOUBLE PRECISION
);
"""

_BATCH_SIZE = 5_000


class RawLoader:
    """Loads extracted pNEUMA records into ``raw.vehicle_trajectories``.

    Parameters
    ----------
    settings:
        Runtime settings containing warehouse connection details.
    dag_id:
        Airflow DAG identifier for audit metadata (optional).
    task_id:
        Airflow task identifier for audit metadata (optional).
    """

    def __init__(
        self,
        settings: Settings,
        dag_id: str = "",
        task_id: str = "",
    ) -> None:
        self._settings = settings
        self._dag_id = dag_id
        self._task_id = task_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_schema(self) -> None:
        """Create destination tables if they do not already exist."""
        wh = self._settings.warehouse
        with psycopg.connect(
            host=wh.host,
            port=wh.port,
            dbname=wh.database,
            user=wh.user,
            password=wh.password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(_DDL_RAW_TABLE)
                cur.execute(_DDL_RAW_INDEX)
                cur.execute(_DDL_AUDIT_TABLE)
            conn.commit()
            log.info("schema ensured: raw.vehicle_trajectories, audit.pipeline_runs")

    def load(
        self,
        source_file: str,
        records: Iterator[PneumaRecord],
    ) -> dict[str, object]:
        """Load *records* for *source_file* into the raw table.

        The load is skipped if the source file has already been successfully
        loaded (idempotency guard).  A partially-loaded file is cleaned up
        and re-loaded.

        Returns a summary dict with ``rows_loaded``, ``rows_rejected``, and
        ``status`` keys.
        """
        run_id = str(uuid.uuid4())
        started_at = time.time()
        wh = self._settings.warehouse

        with psycopg.connect(
            host=wh.host,
            port=wh.port,
            dbname=wh.database,
            user=wh.user,
            password=wh.password,
        ) as conn:
            # ── Idempotency check ─────────────────────────────────────────────
            if self._already_loaded(conn, source_file):
                log.info("skipping %s — already loaded", source_file)
                return {"status": "skipped", "rows_loaded": 0, "rows_rejected": 0}

            # Remove any partial load from a previous failed attempt.
            self._delete_partial(conn, source_file)

            rows_loaded = 0
            rows_rejected = 0
            status = "success"
            error_message = None

            try:
                rows_loaded, rows_rejected = self._bulk_insert(
                    conn, source_file, records
                )
                conn.commit()
                log.info(
                    "loaded %s: %d rows inserted, %d rejected",
                    source_file,
                    rows_loaded,
                    rows_rejected,
                )
            except Exception as exc:
                conn.rollback()
                status = "failed"
                error_message = str(exc)
                log.error("load failed for %s: %s", source_file, exc)
                raise
            finally:
                finished_at = time.time()
                self._write_audit(
                    conn,
                    run_id=run_id,
                    source_file=source_file,
                    status=status,
                    rows_loaded=rows_loaded,
                    rows_rejected=rows_rejected,
                    error_message=error_message,
                    started_at=started_at,
                    finished_at=finished_at,
                )
                conn.commit()

        return {
            "status": status,
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _already_loaded(conn: psycopg.Connection, source_file: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM audit.pipeline_runs
                WHERE source_file = %s AND status = 'success'
                LIMIT 1
                """,
                (source_file,),
            )
            return cur.fetchone() is not None

    @staticmethod
    def _delete_partial(conn: psycopg.Connection, source_file: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM raw.vehicle_trajectories WHERE source_file = %s",
                (source_file,),
            )
        conn.commit()

    @staticmethod
    def _bulk_insert(
        conn: psycopg.Connection,
        source_file: str,
        records: Iterator[PneumaRecord],
    ) -> tuple[int, int]:
        """Insert records in batches; return (rows_loaded, rows_rejected)."""
        insert_sql = """
            INSERT INTO raw.vehicle_trajectories (
                source_file, track_id, vehicle_type,
                traveled_d_m, avg_speed_ms,
                lat, lon, speed_ms, lon_acc_ms2, lat_acc_ms2, timestamp_s
            ) VALUES (
                %(source_file)s, %(track_id)s, %(vehicle_type)s,
                %(traveled_d_m)s, %(avg_speed_ms)s,
                %(lat)s, %(lon)s, %(speed_ms)s,
                %(lon_acc_ms2)s, %(lat_acc_ms2)s, %(timestamp_s)s
            )
        """
        rows_loaded = 0
        rows_rejected = 0
        batch: list[dict] = []

        def flush(cur: psycopg.Cursor) -> None:
            nonlocal rows_loaded
            cur.executemany(insert_sql, batch)
            rows_loaded += len(batch)
            batch.clear()

        with conn.cursor() as cur:
            for record in records:
                batch.append(asdict(record))
                if len(batch) >= _BATCH_SIZE:
                    flush(cur)
            if batch:
                flush(cur)

        return rows_loaded, rows_rejected

    def _write_audit(
        self,
        conn: psycopg.Connection,
        *,
        run_id: str,
        source_file: str,
        status: str,
        rows_loaded: int,
        rows_rejected: int,
        error_message: str | None,
        started_at: float,
        finished_at: float,
    ) -> None:
        import datetime

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.pipeline_runs (
                    run_id, dag_id, task_id, source_file,
                    status, rows_loaded, rows_rejected, error_message,
                    started_at, finished_at, duration_s
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    run_id,
                    self._dag_id,
                    self._task_id,
                    source_file,
                    status,
                    rows_loaded,
                    rows_rejected,
                    error_message,
                    datetime.datetime.fromtimestamp(started_at, tz=datetime.timezone.utc),
                    datetime.datetime.fromtimestamp(finished_at, tz=datetime.timezone.utc),
                    round(finished_at - started_at, 3),
                ),
            )
