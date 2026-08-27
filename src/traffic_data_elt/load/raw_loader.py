"""RawLoader — loads pNEUMA frame records into raw.vehicle_trajectories.

Design decisions
----------------
- Uses ``psycopg`` (v3) with ``executemany`` for efficient bulk inserts.
- Idempotent: the idempotency key is (source_file, file_hash).  A file that
  has been successfully loaded with the same SHA-256 hash is skipped.  A file
  whose content has changed (new hash) is treated as a new load — the old rows
  are deleted and the new rows are inserted in the same transaction.
- Partial-load safety: DELETE and INSERT share a single database transaction.
  If the INSERT fails the DELETE is also rolled back, leaving the previously
  loaded rows intact.
- All connection credentials come from :class:`~traffic_data_elt.config.Settings`;
  they are never logged.
- Audit metadata is written to ``audit.pipeline_runs`` on success or failure
  so every load attempt is observable.
"""

from __future__ import annotations

import datetime
import hashlib
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Iterator

import psycopg

from traffic_data_elt.config import Settings
from traffic_data_elt.extract.pneuma import PneumaRecord
from traffic_data_elt.utils import get_logger

log = get_logger(__name__)

_DDL_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS raw.vehicle_trajectories (
    id                BIGSERIAL PRIMARY KEY,
    source_file       TEXT             NOT NULL,
    file_hash         TEXT             NOT NULL,
    track_id          INTEGER          NOT NULL,
    vehicle_type      TEXT             NOT NULL,
    traveled_d_m      DOUBLE PRECISION NOT NULL,
    avg_speed_ms      DOUBLE PRECISION NOT NULL,
    lat               DOUBLE PRECISION NOT NULL,
    lon               DOUBLE PRECISION NOT NULL,
    speed_ms          DOUBLE PRECISION NOT NULL,
    lon_acc_ms2       DOUBLE PRECISION NOT NULL,
    lat_acc_ms2       DOUBLE PRECISION NOT NULL,
    timestamp_s       DOUBLE PRECISION NOT NULL,
    ingested_at       TIMESTAMPTZ      NOT NULL DEFAULT now()
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
    file_hash         TEXT,
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


def _sha256(path: Path) -> str:
    """Return the hex SHA-256 digest of a file."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


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
        file_path: str | Path,
        records: Iterator[PneumaRecord],
    ) -> dict[str, object]:
        """Load *records* for the file at *file_path* into the raw table.

        Idempotency
        -----------
        The key is ``(source_file, file_hash)``.  If a successful run already
        exists for this exact (name, content) pair the load is skipped.

        If the file content has changed (same name, different hash) the old
        rows are deleted and the new rows are inserted atomically — both
        operations share a single transaction so a mid-load failure leaves the
        previously loaded rows intact.

        Returns a summary dict with ``rows_loaded``, ``rows_rejected``, and
        ``status`` keys.
        """
        path = Path(file_path)
        source_file = path.name
        file_hash = _sha256(path)

        run_id = str(uuid.uuid4())
        started_at = time.monotonic()
        started_wall = datetime.datetime.now(tz=datetime.timezone.utc)
        wh = self._settings.warehouse

        rows_loaded = 0
        rows_rejected = 0
        status = "success"
        error_message = None

        with psycopg.connect(
            host=wh.host,
            port=wh.port,
            dbname=wh.database,
            user=wh.user,
            password=wh.password,
            autocommit=True,
        ) as conn:
            # ── Idempotency check ─────────────────────────────────────────────
            if self._already_loaded(conn, source_file, file_hash):
                log.info("skipping %s (%s) — already loaded", source_file, file_hash[:12])
                return {"status": "skipped", "rows_loaded": 0, "rows_rejected": 0}

            try:
                # DELETE old rows for this filename (if any) and INSERT new rows
                # in a single transaction.  Rolling back on failure restores any
                # previously loaded rows for this file.
                with conn.transaction():
                    self._delete_by_source(conn, source_file)
                    rows_loaded, rows_rejected = self._bulk_insert(
                        conn, source_file, file_hash, records
                    )

                log.info(
                    "loaded %s: %d rows inserted, %d rejected",
                    source_file,
                    rows_loaded,
                    rows_rejected,
                )
            except Exception as exc:
                status = "failed"
                error_message = str(exc)
                log.error("load failed for %s: %s", source_file, exc)
                raise
            finally:
                finished_wall = datetime.datetime.now(tz=datetime.timezone.utc)
                duration = time.monotonic() - started_at
                # Audit write uses autocommit=True so it succeeds even after a
                # rolled-back transaction.
                self._write_audit(
                    conn,
                    run_id=run_id,
                    source_file=source_file,
                    file_hash=file_hash,
                    status=status,
                    rows_loaded=rows_loaded,
                    rows_rejected=rows_rejected,
                    error_message=error_message,
                    started_at=started_wall,
                    finished_at=finished_wall,
                    duration_s=round(duration, 3),
                )

        return {
            "status": status,
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _already_loaded(
        conn: psycopg.Connection, source_file: str, file_hash: str
    ) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM audit.pipeline_runs
                WHERE source_file = %s
                  AND file_hash   = %s
                  AND status      = 'success'
                LIMIT 1
                """,
                (source_file, file_hash),
            )
            return cur.fetchone() is not None

    @staticmethod
    def _delete_by_source(conn: psycopg.Connection, source_file: str) -> None:
        """Delete all rows for *source_file*.  Must be called inside a transaction."""
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM raw.vehicle_trajectories WHERE source_file = %s",
                (source_file,),
            )

    @staticmethod
    def _bulk_insert(
        conn: psycopg.Connection,
        source_file: str,
        file_hash: str,
        records: Iterator[PneumaRecord],
    ) -> tuple[int, int]:
        """Insert records in batches.  Must be called inside a transaction.

        Returns (rows_loaded, rows_rejected).  rows_rejected is currently
        always 0 because bad records are rejected upstream by PneumaExtractor;
        the counter is retained for future per-row validation use.
        """
        insert_sql = """
            INSERT INTO raw.vehicle_trajectories (
                source_file, file_hash, track_id, vehicle_type,
                traveled_d_m, avg_speed_ms,
                lat, lon, speed_ms, lon_acc_ms2, lat_acc_ms2, timestamp_s
            ) VALUES (
                %(source_file)s, %(file_hash)s, %(track_id)s, %(vehicle_type)s,
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
                row = asdict(record)
                row["source_file"] = source_file
                row["file_hash"] = file_hash
                batch.append(row)
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
        file_hash: str,
        status: str,
        rows_loaded: int,
        rows_rejected: int,
        error_message: str | None,
        started_at: datetime.datetime,
        finished_at: datetime.datetime,
        duration_s: float,
    ) -> None:
        # Use autocommit for the audit write so it is committed regardless of
        # whether the data load transaction was rolled back.
        old_autocommit = conn.autocommit
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO audit.pipeline_runs (
                        run_id, dag_id, task_id, source_file, file_hash,
                        status, rows_loaded, rows_rejected, error_message,
                        started_at, finished_at, duration_s
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    """,
                    (
                        run_id,
                        self._dag_id,
                        self._task_id,
                        source_file,
                        file_hash,
                        status,
                        rows_loaded,
                        rows_rejected,
                        error_message,
                        started_at,
                        finished_at,
                        duration_s,
                    ),
                )
        except psycopg.Error as exc:
            log.error("failed to write audit record for %s: %s", source_file, exc)
            raise
        
