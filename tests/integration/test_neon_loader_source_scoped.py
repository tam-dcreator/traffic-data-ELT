"""Opt-in integration test: replace_sources source-scoped semantics.

Proves the ``neon_loader`` ``replace_sources`` load-mode contract against a
**real PostgreSQL** (the local V1 warehouse or any reachable Postgres) using
**synthetic** data — no Spark, no Neon, no pNEUMA dataset, and it never touches
Neon production.

What it proves
--------------
1. Source preservation: loading a replacement batch for ``source_A`` replaces
   only ``source_A`` and leaves an unrelated ``source_B`` completely unchanged.
2. Idempotent rerun: loading the same source twice yields no duplicate rows and
   a stable row count.

Isolation
---------
Runs entirely inside a disposable schema (default ``serving_srctest``) on a
local Postgres, created and dropped by the test.  It connects using the V1
warehouse env vars (``TRAFFIC_DB_*``) or explicit ``SERVING_TEST_PG_*`` vars;
it is skipped automatically when neither is available (so the normal unit run
does not require a database).

A tiny ``_FakeGoldDF`` stands in for a Spark DataFrame — the loader only needs
``.select(*cols).toLocalIterator()`` yielding column-addressable rows, so no
PySpark install is required.

Run explicitly (V1 warehouse up on localhost:5432):
    TRAFFIC_DB_NAME=... TRAFFIC_DB_USER=... TRAFFIC_DB_PASSWORD=... \\
    pytest tests/integration/test_neon_loader_source_scoped.py -v -s
"""

from __future__ import annotations

import os
import sys
import types

import pytest


def _ensure_pyspark_functions_col() -> None:
    """Ensure ``pyspark.sql.functions.col`` is callable for the loader's
    ``_spark_row_iter`` when run against the _FakeGoldDF.

    Called INSIDE the tests (not at import) so it never affects collection of
    other test modules (e.g. the Silver integration test that probes for a real
    ``SparkSession``).  If pyspark is already present it is left untouched; only
    a missing ``functions.col`` is filled in with an identity function (the fake
    DF ignores the projection args).
    """
    if "pyspark" not in sys.modules:
        pyspark = types.ModuleType("pyspark")
        pyspark_sql = types.ModuleType("pyspark.sql")
        pyspark.sql = pyspark_sql
        sys.modules["pyspark"] = pyspark
        sys.modules["pyspark.sql"] = pyspark_sql
    try:
        from pyspark.sql import functions as _f  # noqa: PLC0415
        if not hasattr(_f, "col"):
            _f.col = lambda name: name
    except ModuleNotFoundError:
        fns = types.ModuleType("pyspark.sql.functions")
        fns.col = lambda name: name
        sys.modules["pyspark.sql"].functions = fns
        sys.modules["pyspark.sql.functions"] = fns


# ── Skip guards ──────────────────────────────────────────────────────────────
try:
    import psycopg  # noqa: F401
    _HAS_PSYCOPG = True
except ModuleNotFoundError:
    _HAS_PSYCOPG = False


def _conninfo() -> dict | None:
    """Build a local-Postgres conninfo from env, or None if unavailable."""
    host = os.environ.get("SERVING_TEST_PG_HOST") or os.environ.get("TRAFFIC_DB_HOST", "localhost")
    port = os.environ.get("SERVING_TEST_PG_PORT") or os.environ.get("TRAFFIC_DB_PORT", "5432")
    dbname = os.environ.get("SERVING_TEST_PG_NAME") or os.environ.get("TRAFFIC_DB_NAME")
    user = os.environ.get("SERVING_TEST_PG_USER") or os.environ.get("TRAFFIC_DB_USER")
    password = os.environ.get("SERVING_TEST_PG_PASSWORD") or os.environ.get("TRAFFIC_DB_PASSWORD")
    if not (dbname and user and password is not None):
        return None
    return {"host": host, "port": int(port), "dbname": dbname,
            "user": user, "password": password}


_CONNINFO = _conninfo() if _HAS_PSYCOPG else None

pytestmark = pytest.mark.skipif(
    _CONNINFO is None,
    reason="source-scoped loader test skipped: no local Postgres "
           "(set TRAFFIC_DB_* or SERVING_TEST_PG_*) or psycopg missing",
)

# A disposable schema so we never collide with real serving data.
_TEST_SCHEMA = os.environ.get("SERVING_TEST_SCHEMA", "serving_srctest")

# Synthetic column order must match the serving contract (checked in the test).
from traffic_data_elt.databricks.schemas import serving_schema as ss  # noqa: E402


class _FakeRow:
    """Column-addressable row (``row[col]``) — mimics a Spark Row."""

    def __init__(self, values: dict):
        self._v = values

    def __getitem__(self, col):
        return self._v[col]


class _FakeGoldDF:
    """Minimal Spark-DataFrame stand-in for the loader.

    Supports only what ``neon_loader`` uses: ``.select(*cols)`` (returns self)
    and ``.toLocalIterator()`` (yields column-addressable rows in contract
    order).
    """

    def __init__(self, rows: list[dict]):
        self._rows = rows

    def select(self, *cols):  # noqa: ARG002 - column identity handled by rows
        return self

    def toLocalIterator(self):  # noqa: N802 - Spark API name
        for r in self._rows:
            yield _FakeRow(r)


def _traj(source_file: str, track_id: int, **over) -> dict:
    """One synthetic serving-contract row (all 19 columns)."""
    base = {
        "source_file": source_file,
        "track_id": track_id,
        "vehicle_type": "car",
        "frame_count": 100,
        "start_time_s": 0.0,
        "end_time_s": 4.0,
        "duration_s": 4.0,
        "traveled_d_m": 48.85,
        "avg_speed_ms": 9.77,
        "max_speed_ms": 12.5,
        "min_speed_ms": 0.0,
        "avg_lon_acc_ms2": 0.01,
        "avg_lat_acc_ms2": -0.02,
        "max_lon_acc_ms2": 1.2,
        "max_lat_acc_ms2": 0.9,
        "start_lat": 37.98,
        "start_lon": 23.73,
        "end_lat": 37.99,
        "end_lon": 23.74,
    }
    base.update(over)
    return base


@pytest.fixture()
def clean_schema():
    """Create a disposable test schema; drop it (and contents) afterwards."""
    import psycopg
    with psycopg.connect(connect_timeout=15, **_CONNINFO) as conn, conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE")
        conn.commit()
    yield _TEST_SCHEMA
    with psycopg.connect(connect_timeout=15, **_CONNINFO) as conn, conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE")
        conn.commit()


def _count(cur, schema, source_file=None) -> int:
    if source_file is None:
        cur.execute(f"SELECT count(*) FROM {schema}.{ss.SERVING_TABLE}")
    else:
        cur.execute(
            f"SELECT count(*) FROM {schema}.{ss.SERVING_TABLE} WHERE source_file = %s",
            (source_file,),
        )
    return cur.fetchone()[0]


def test_column_order_matches_contract():
    # Guard: the synthetic row must cover exactly the serving contract columns.
    assert set(_traj("a", 1).keys()) == set(ss.SERVING_COLUMN_NAMES)


def test_replace_sources_preserves_unrelated_sources(clean_schema):
    """source_A replaced; source_B left completely unchanged."""
    import psycopg

    _ensure_pyspark_functions_col()
    from traffic_data_elt.databricks.neon_loader import load_gold_to_neon

    schema = clean_schema

    # ── Initial load: source_A (2 rows) + source_B (3 rows) ──────────────────
    initial = _FakeGoldDF(
        [_traj("source_A.csv", 1), _traj("source_A.csv", 2)]
        + [_traj("source_B.csv", 1), _traj("source_B.csv", 2), _traj("source_B.csv", 3)]
    )
    r0 = load_gold_to_neon(initial, dict(_CONNINFO), run_id="rinit", schema=schema)
    assert r0.status == "success"

    with psycopg.connect(**_CONNINFO) as conn, conn.cursor() as cur:
        assert _count(cur, schema) == 5
        assert _count(cur, schema, "source_A.csv") == 2
        assert _count(cur, schema, "source_B.csv") == 3
        # Snapshot source_B rows to prove they are byte-for-byte untouched.
        cur.execute(
            f"SELECT track_id, vehicle_type, frame_count FROM {schema}.{ss.SERVING_TABLE} "
            f"WHERE source_file = 'source_B.csv' ORDER BY track_id"
        )
        b_before = cur.fetchall()

    # ── Replacement batch: ONLY source_A, now a DIFFERENT complete set ───────
    replacement = _FakeGoldDF([
        _traj("source_A.csv", 10, vehicle_type="taxi", frame_count=250),
        _traj("source_A.csv", 11, vehicle_type="bus", frame_count=300),
        _traj("source_A.csv", 12, vehicle_type="car", frame_count=150),
    ])
    r1 = load_gold_to_neon(replacement, dict(_CONNINFO), run_id="rrepl", schema=schema)
    assert r1.status == "success"
    assert r1.source_files == ["source_A.csv"]

    with psycopg.connect(**_CONNINFO) as conn, conn.cursor() as cur:
        # source_A fully replaced: old track_ids 1,2 gone; new 10,11,12 present.
        cur.execute(
            f"SELECT track_id FROM {schema}.{ss.SERVING_TABLE} "
            f"WHERE source_file = 'source_A.csv' ORDER BY track_id"
        )
        a_after = [row[0] for row in cur.fetchall()]
        assert a_after == [10, 11, 12]

        # source_B COMPLETELY unchanged (count + exact rows).
        assert _count(cur, schema, "source_B.csv") == 3
        cur.execute(
            f"SELECT track_id, vehicle_type, frame_count FROM {schema}.{ss.SERVING_TABLE} "
            f"WHERE source_file = 'source_B.csv' ORDER BY track_id"
        )
        assert cur.fetchall() == b_before

        # Total = replaced A (3) + preserved B (3).
        assert _count(cur, schema) == 6
        # No duplicate grain anywhere.
        cur.execute(
            f"SELECT count(*) FROM (SELECT source_file, track_id "
            f"FROM {schema}.{ss.SERVING_TABLE} GROUP BY source_file, track_id "
            f"HAVING count(*) > 1) d"
        )
        assert cur.fetchone()[0] == 0


def test_replace_sources_rerun_is_idempotent(clean_schema):
    """Loading the SAME source twice → no duplicates, stable count."""
    import psycopg

    _ensure_pyspark_functions_col()
    from traffic_data_elt.databricks.neon_loader import load_gold_to_neon

    schema = clean_schema
    batch = _FakeGoldDF([_traj("source_A.csv", i) for i in range(1, 6)])  # 5 rows

    r1 = load_gold_to_neon(batch, dict(_CONNINFO), run_id="run1", schema=schema)
    assert r1.status == "success"
    # Rerun the identical source batch.
    r2 = load_gold_to_neon(
        _FakeGoldDF([_traj("source_A.csv", i) for i in range(1, 6)]),
        dict(_CONNINFO), run_id="run2", schema=schema,
    )
    assert r2.status == "success"

    with psycopg.connect(**_CONNINFO) as conn, conn.cursor() as cur:
        assert _count(cur, schema) == 5  # NOT 10
        cur.execute(
            f"SELECT count(*) FROM (SELECT source_file, track_id "
            f"FROM {schema}.{ss.SERVING_TABLE} GROUP BY source_file, track_id "
            f"HAVING count(*) > 1) d"
        )
        assert cur.fetchone()[0] == 0  # no duplicate grain
