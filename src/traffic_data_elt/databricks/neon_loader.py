"""Gold → Neon serving loader for the V2 serving layer.

Responsibility
--------------
Move the validated Spark Gold ``trajectory_summary`` DataFrame into the Neon
``serving.gold_trajectory_summary`` table using a safe staging/publish pattern:

    Spark Gold DataFrame
        → stream rows to the driver in bounded batches (toLocalIterator)
        → COPY each batch into a run-scoped staging table
        → validate staging (invariants; optional fixture expectations)
        → publish into serving.<table> according to the load mode
        → clean up the staging table

This module contains **no** parsing, Silver, Gold-aggregation, or dbt-mart
logic — only the relational load/validate/publish handoff.  The column and type
contract lives in ``schemas/serving_schema.py``; the loader never redefines it.

Bounded driver memory
----------------------
Rows are streamed with ``DataFrame.toLocalIterator()`` and pushed to Neon with
``COPY`` in configurable batches (``copy_batch_size``), so driver memory stays
bounded even for hundreds of thousands of trajectories.  We do **not**
``collect()`` the whole DataFrame, and we do not open connections from Spark
executors — a single driver-side psycopg connection performs the COPY.

Load modes
----------
``replace_sources`` (default, production-safe)
    Source-scoped upsert-by-replace.  Within one transaction, delete existing
    serving rows whose ``source_file`` appears in the staged batch, then insert
    the staged rows.  New source files are preserved; reprocessing a source
    replaces only that source's rows; rerunning the same source is idempotent.

    INPUT CONTRACT (important):
    For every ``source_file`` present in the incoming Gold DataFrame, that
    DataFrame MUST contain the **complete current Gold trajectory set** for
    that source file.  ``replace_sources`` deletes all existing serving rows
    for each incoming source before inserting the replacement, so a partial
    subset of an existing source file would silently DROP the trajectories that
    were omitted from the batch.  Do NOT call ``replace_sources`` with a partial
    slice of a source file's trajectories.  Source files NOT present in the
    batch are never touched.

``replace_snapshot`` (explicit full refresh / testing only)
    ``TRUNCATE`` + ``INSERT`` in one transaction — replaces the entire serving
    table.  Must be requested explicitly; never the default.

Transaction / locking notes
---------------------------
- ``replace_sources`` uses ``DELETE ... WHERE source_file IN (...)`` + ``INSERT``
  inside a single transaction.  This is transactionally atomic and takes only
  row/table ``ROW EXCLUSIVE`` locks; concurrent readers see the pre-commit rows
  under MVCC until COMMIT.  A failure rolls back, leaving the serving table
  intact.
- ``replace_snapshot`` uses ``TRUNCATE``, which is transactional in PostgreSQL
  but acquires an ``ACCESS EXCLUSIVE`` lock on the serving table for the
  duration of the transaction (it is NOT a lock-free/MVCC operation).  It is
  therefore reserved for explicit full-refresh runs.
- Single-writer assumption: this loader assumes one serving writer at a time
  (the pipeline is not run concurrently against the same serving table).  No
  distributed locking is added; concurrent writers to the same source_file set
  are out of scope for this milestone.

Connectivity
------------
Connects to Neon from the Databricks driver with ``psycopg`` over TLS
(``sslmode=require``).  Credentials are supplied by the caller as a plain dict
(built from Databricks secrets) — the password is never logged or printed.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable, Iterator

from traffic_data_elt.databricks.schemas import serving_schema as ss
from traffic_data_elt.utils import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

log = get_logger(__name__)

# Default COPY batch size (rows streamed to the driver + COPY'd per round).
# Bounded so driver memory stays flat regardless of total dataset size.
DEFAULT_COPY_BATCH_SIZE = 10_000

# Supported load modes.
LOAD_MODE_REPLACE_SOURCES = "replace_sources"
LOAD_MODE_REPLACE_SNAPSHOT = "replace_snapshot"
VALID_LOAD_MODES = (LOAD_MODE_REPLACE_SOURCES, LOAD_MODE_REPLACE_SNAPSHOT)


@dataclass
class NeonLoadResult:
    """Outcome metadata for a Gold → Neon serving load."""

    serving_table: str
    staging_table: str
    run_id: str
    load_mode: str
    staged_row_count: int
    published_row_count: int
    sum_frame_count: int
    distinct_grain: int
    source_files: list[str]
    start_time: datetime.datetime
    end_time: datetime.datetime
    status: str  # "success" | "failed"
    validation_passed: bool = False
    staging_cleaned: bool = False
    error: str | None = None
    validation_detail: dict = field(default_factory=dict)

    def frames_conserved(self, expected_frame_sum: int) -> bool:
        """True when SUM(frame_count) equals the caller-supplied expectation.

        Frame conservation is only meaningful against a known expected total
        (e.g. an integration fixture).  Production callers that do not know the
        total ahead of time should not use this check.
        """
        return self.sum_frame_count == expected_frame_sum


class NeonLoadError(RuntimeError):
    """Raised for fatal loader errors (connection, DDL, publish)."""


# ---------------------------------------------------------------------------
# Pure helpers (unit-testable without psycopg / Spark)
# ---------------------------------------------------------------------------


def iter_row_batches(
    row_iter: Iterable[tuple],
    batch_size: int = DEFAULT_COPY_BATCH_SIZE,
) -> Iterator[list[tuple]]:
    """Yield bounded batches (lists) of row tuples from an iterable.

    Pure and Spark-free: works on any iterable of tuples, so batching behaviour
    is unit-testable without Spark or Neon.  ``batch_size`` must be positive.
    """
    if batch_size <= 0:
        raise ValueError(f"batch_size must be positive, got {batch_size}")
    batch: list[tuple] = []
    for row in row_iter:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _spark_row_iter(gold_df: "DataFrame") -> Iterator[tuple]:
    """Stream a Spark Gold DataFrame to the driver as ordered row tuples.

    Uses ``toLocalIterator`` so only one partition at a time is held on the
    driver — bounded memory, no full ``collect()``.  Columns are projected in
    the serving contract order so tuples line up with the COPY column list.
    """
    from pyspark.sql import functions as F  # noqa: PLC0415

    ordered = gold_df.select(*[F.col(c) for c in ss.SERVING_COLUMN_NAMES])
    for r in ordered.toLocalIterator():
        yield tuple(r[c] for c in ss.SERVING_COLUMN_NAMES)


def build_insert_sql(qualified_table: str) -> str:
    """Return a parameterised INSERT for the serving column contract."""
    cols = ss.insert_columns_csv()
    placeholders = ", ".join(["%s"] * len(ss.SERVING_COLUMN_NAMES))
    return f"INSERT INTO {qualified_table} ({cols}) VALUES ({placeholders})"


def copy_sql(qualified_table: str) -> str:
    """Return a ``COPY ... FROM STDIN`` statement for the serving columns."""
    return f"COPY {qualified_table} ({ss.insert_columns_csv()}) FROM STDIN"


def evaluate_validation(
    results: dict[str, int],
    *,
    expected_row_count: int | None = None,
    expected_frame_sum: int | None = None,
) -> tuple[bool, dict]:
    """Evaluate serving validation scalars.

    Always enforces **data invariants** that must hold for any valid batch:
      - grain uniqueness (distinct grain == row count)
      - row count > 0
      - frame_count > 0 on every row
      - duration/distance/speed non-negative, start_time <= end_time
      - grain columns not null

    When ``expected_row_count`` / ``expected_frame_sum`` are supplied (fixture /
    integration mode) they are additionally checked exactly.  In production mode
    they are omitted and only invariants are enforced, so the loader works for
    any batch size (not just the 922-row fixture).

    Returns ``(passed, detail)`` where detail maps each check to ``(value, ok)``.
    """
    row_count = results.get("row_count")
    distinct_grain = results.get("distinct_grain")

    checks: dict[str, tuple] = {}
    # ── Invariants (always) ──────────────────────────────────────────────────
    checks["row_count_positive"] = (row_count, (row_count or 0) > 0)
    checks["grain_unique"] = (distinct_grain == row_count, distinct_grain == row_count)
    checks["frame_count_positive"] = (
        results.get("min_frame_count"), (results.get("min_frame_count") or 0) > 0
    )
    checks["duration_non_negative"] = (
        results.get("bad_duration"), results.get("bad_duration") == 0
    )
    checks["distance_non_negative"] = (
        results.get("bad_distance"), results.get("bad_distance") == 0
    )
    checks["speed_non_negative"] = (
        results.get("bad_speed"), results.get("bad_speed") == 0
    )
    checks["time_order"] = (
        results.get("bad_time_order"), results.get("bad_time_order") == 0
    )
    checks["grain_not_null"] = (
        results.get("null_grain"), results.get("null_grain") == 0
    )

    # ── Optional fixture expectations ────────────────────────────────────────
    if expected_row_count is not None:
        checks["expected_row_count"] = (row_count, row_count == expected_row_count)
    if expected_frame_sum is not None:
        checks["expected_frame_sum"] = (
            results.get("sum_frame_count"),
            results.get("sum_frame_count") == expected_frame_sum,
        )

    passed = all(ok for _, ok in checks.values())
    return passed, checks


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_gold_to_neon(
    gold_df: "DataFrame",
    conninfo: dict,
    *,
    run_id: str,
    load_mode: str = LOAD_MODE_REPLACE_SOURCES,
    copy_batch_size: int = DEFAULT_COPY_BATCH_SIZE,
    schema: str = ss.SERVING_SCHEMA,
    table: str = ss.SERVING_TABLE,
    expected_row_count: int | None = None,
    expected_frame_sum: int | None = None,
) -> NeonLoadResult:
    """Load a Spark Gold DataFrame into the Neon serving table.

    Parameters
    ----------
    gold_df:
        Validated Spark Gold ``trajectory_summary`` DataFrame.
    conninfo:
        psycopg connection kwargs (host/port/dbname/user/password/sslmode).
        Contains the password — never logged here.
    run_id:
        Unique identifier-safe run id used for the staging table name.
    load_mode:
        ``replace_sources`` (default) or ``replace_snapshot`` (explicit).
        WARNING for ``replace_sources``: ``gold_df`` must carry the COMPLETE
        current trajectory set for every ``source_file`` it contains — existing
        serving rows for those sources are deleted before insert.  Never pass a
        partial subset of an existing source file (see module docstring).
    copy_batch_size:
        Rows per COPY batch (bounded driver memory).  Default 10,000.
    schema, table:
        Serving schema/table names (defaults from the contract).
    expected_row_count, expected_frame_sum:
        Optional fixture/integration expectations.  When ``None`` (production)
        only data invariants are enforced.

    Returns
    -------
    NeonLoadResult

    Raises
    ------
    NeonLoadError
        On connection/DDL/validation/publish failure (staging retained for
        diagnosis; serving table left intact).
    """
    import psycopg  # noqa: PLC0415

    if load_mode not in VALID_LOAD_MODES:
        raise NeonLoadError(
            f"invalid load_mode {load_mode!r}; expected one of {VALID_LOAD_MODES}"
        )

    start_time = datetime.datetime.now(tz=datetime.timezone.utc)
    serving_fq = ss.qualified_serving_table(schema, table)
    staging_fq = ss.qualified_staging_table(run_id, schema, table)

    log.info(
        "neon load starting: run_id=%s mode=%s serving=%s batch=%d",
        run_id, load_mode, serving_fq, copy_batch_size,
    )

    result = NeonLoadResult(
        serving_table=serving_fq,
        staging_table=staging_fq,
        run_id=run_id,
        load_mode=load_mode,
        staged_row_count=0,
        published_row_count=0,
        sum_frame_count=0,
        distinct_grain=0,
        source_files=[],
        start_time=start_time,
        end_time=start_time,
        status="failed",
    )

    conn = None
    try:
        conn = psycopg.connect(connect_timeout=30, **conninfo)

        # ── 1. Ensure schema + serving table + fresh staging table ───────────
        with conn.cursor() as cur:
            cur.execute(ss.create_schema_sql(schema))
            cur.execute(ss.create_table_sql(serving_fq, with_pk=True))  # first run
            cur.execute(ss.drop_staging_table_sql(run_id, schema))  # idempotent rerun
            cur.execute(ss.create_staging_table_sql(run_id, schema))
        conn.commit()

        # ── 2. Bounded COPY into staging (no full collect) ───────────────────
        staged = 0
        copy_stmt = copy_sql(staging_fq)
        with conn.cursor() as cur:
            for batch in iter_row_batches(_spark_row_iter(gold_df), copy_batch_size):
                with cur.copy(copy_stmt) as cp:
                    for row in batch:
                        cp.write_row(row)
                staged += len(batch)
        conn.commit()
        result.staged_row_count = staged
        log.info("staged %d rows into %s (batched COPY)", staged, staging_fq)

        # ── 3. Validate staging BEFORE publish ───────────────────────────────
        scalars = _run_validation_queries(conn, staging_fq)
        passed, detail = evaluate_validation(
            scalars,
            expected_row_count=expected_row_count,
            expected_frame_sum=expected_frame_sum,
        )
        result.validation_passed = passed
        result.validation_detail = detail
        result.sum_frame_count = int(scalars.get("sum_frame_count") or 0)
        result.distinct_grain = int(scalars.get("distinct_grain") or 0)
        result.source_files = _distinct_source_files(conn, staging_fq)

        if not passed:
            failed = [k for k, (_, ok) in detail.items() if not ok]
            result.error = f"staging validation failed: {failed}"
            log.error("staging validation FAILED (%s): %s", staging_fq, failed)
            raise NeonLoadError(result.error)  # staging retained; serving untouched
        log.info("staging validation passed for %s", staging_fq)

        # ── 4. Publish per load mode (single transaction) ────────────────────
        with conn.transaction():
            with conn.cursor() as cur:
                if load_mode == LOAD_MODE_REPLACE_SNAPSHOT:
                    # Explicit full refresh — ACCESS EXCLUSIVE lock via TRUNCATE.
                    cur.execute(f"TRUNCATE {serving_fq};")
                else:
                    # Source-scoped replace (default): delete only the source
                    # files present in this batch, then insert.  Preserves other
                    # sources; idempotent for reruns of the same source(s).
                    # INPUT CONTRACT: the batch must contain the COMPLETE current
                    # trajectory set for every source_file it carries — this
                    # DELETE removes ALL existing rows for those sources first,
                    # so a partial subset would drop the omitted trajectories.
                    cur.execute(
                        f"DELETE FROM {serving_fq} s "
                        f"USING (SELECT DISTINCT source_file FROM {staging_fq}) b "
                        f"WHERE s.source_file = b.source_file;"
                    )
                cur.execute(
                    f"INSERT INTO {serving_fq} ({ss.insert_columns_csv()}) "
                    f"SELECT {ss.insert_columns_csv()} FROM {staging_fq};"
                )
        # transaction committed on context-manager exit

        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {serving_fq};")
            result.published_row_count = cur.fetchone()[0]
        conn.commit()
        log.info(
            "published (mode=%s): serving now has %d rows",
            load_mode, result.published_row_count,
        )

        # ── 5. Cleanup staging (only after successful publish) ───────────────
        with conn.cursor() as cur:
            cur.execute(ss.drop_staging_table_sql(run_id, schema))
        conn.commit()
        result.staging_cleaned = True

        result.status = "success"
        result.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
        return result

    except NeonLoadError:
        if conn is not None:
            conn.rollback()
        result.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
        raise
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        result.error = str(exc)
        result.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
        log.error("neon load failed: %s", exc)
        raise NeonLoadError(str(exc)) from exc
    finally:
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# Internal query helpers (psycopg-dependent)
# ---------------------------------------------------------------------------


def _run_validation_queries(conn, qualified_table: str) -> dict[str, int]:
    scalars: dict[str, int] = {}
    with conn.cursor() as cur:
        for name, sql in ss.validation_queries(qualified_table).items():
            cur.execute(sql)
            scalars[name] = cur.fetchone()[0]
    return scalars


def _distinct_source_files(conn, qualified_table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT source_file FROM {qualified_table} ORDER BY source_file;")
        return [r[0] for r in cur.fetchall()]


def measure_storage(conninfo: dict, *, schema: str = ss.SERVING_SCHEMA,
                    table: str = ss.SERVING_TABLE) -> dict[str, int]:
    """Query real PostgreSQL storage metadata for the serving table.

    Returns a dict of ``table_bytes``, ``index_bytes``, ``total_bytes``,
    ``database_bytes``.  Read-only; safe to call after publish.
    """
    import psycopg  # noqa: PLC0415

    serving_fq = ss.qualified_serving_table(schema, table)
    out: dict[str, int] = {}
    conn = psycopg.connect(connect_timeout=30, **conninfo)
    try:
        with conn.cursor() as cur:
            for name, sql in ss.storage_queries(serving_fq).items():
                cur.execute(sql)
                out[name] = int(cur.fetchone()[0])
    finally:
        conn.close()
    return out


def bytes_per_trajectory(total_bytes: int, row_count: int) -> float:
    """Bytes per trajectory row (0 when row_count is 0)."""
    return total_bytes / row_count if row_count else 0.0


def project_full_storage(total_bytes: int, row_count: int, target_rows: int = 500_000) -> int:
    """Project total serving bytes for *target_rows* using the fixture baseline.

    Linear projection from the measured fixture — labelled a projection by the
    caller, not a guarantee.
    """
    if row_count <= 0:
        return 0
    return round(total_bytes / row_count * target_rows)
