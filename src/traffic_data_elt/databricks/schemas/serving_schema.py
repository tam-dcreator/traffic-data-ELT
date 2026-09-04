"""PostgreSQL serving-table contract for the V2 Neon serving layer.

This module is the single source of truth for the physical Neon serving table
``serving.gold_trajectory_summary`` — the relational handoff of the Spark Gold
``trajectory_summary`` dataset (see ``GOLD_CONTRACT.md``).

It is **PySpark-free and psycopg-free** so it can be imported in local unit
tests without any cloud dependency.  It provides:

- the ordered column → PostgreSQL type contract (19 columns, matching Gold);
- schema / table / staging-table naming helpers;
- DDL builders (schema, table, unique constraint);
- the serving validation query set.

Type mapping (Gold Spark → PostgreSQL), all scalar, no JSON/nested types:

    StringType   → text
    IntegerType  → integer
    LongType     → bigint
    DoubleType   → double precision
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Column contract (Spark Gold type name → PostgreSQL type)
# ---------------------------------------------------------------------------

_SPARK_TO_PG = {
    "StringType": "text",
    "IntegerType": "integer",
    "LongType": "bigint",
    "DoubleType": "double precision",
}

# Ordered (column, postgres_type, not_null) — identical column set/order to the
# Gold contract in v2_cloud/databricks/schemas/gold_schema.py.
_SERVING_COLUMNS: list[tuple[str, str, bool]] = [
    ("source_file",      "text",             True),
    ("track_id",         "integer",          True),
    ("vehicle_type",     "text",             True),
    ("frame_count",      "bigint",           True),
    ("start_time_s",     "double precision", True),
    ("end_time_s",       "double precision", True),
    ("duration_s",       "double precision", True),
    ("traveled_d_m",     "double precision", True),
    ("avg_speed_ms",     "double precision", True),
    ("max_speed_ms",     "double precision", True),
    ("min_speed_ms",     "double precision", True),
    ("avg_lon_acc_ms2",  "double precision", True),
    ("avg_lat_acc_ms2",  "double precision", True),
    ("max_lon_acc_ms2",  "double precision", True),
    ("max_lat_acc_ms2",  "double precision", True),
    ("start_lat",        "double precision", True),
    ("start_lon",        "double precision", True),
    ("end_lat",          "double precision", True),
    ("end_lon",          "double precision", True),
]

SERVING_COLUMN_NAMES: list[str] = [c for c, _, _ in _SERVING_COLUMNS]
SERVING_NOT_NULL_COLUMNS: list[str] = [c for c, _, nn in _SERVING_COLUMNS if nn]
SERVING_GRAIN_KEYS: list[str] = ["source_file", "track_id"]

# Naming
SERVING_SCHEMA = "serving"
SERVING_TABLE = "gold_trajectory_summary"

# Column count of the fixed Gold/Neon trajectory-summary contract.
#
# This is the width of the SERVING (Gold-derived) relational contract — one row
# per (source_file, track_id) — NOT the variable-width raw pNEUMA source record.
# Derived from _SERVING_COLUMNS so it can never drift from the actual contract.
SERVING_COLUMN_COUNT = len(_SERVING_COLUMNS)

_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


# ---------------------------------------------------------------------------
# Identifier / naming helpers
# ---------------------------------------------------------------------------


def _safe_ident(name: str) -> str:
    """Validate a SQL identifier fragment (letters, digits, underscore only).

    Raises ``ValueError`` on anything else so run-id-derived names can never
    inject SQL.  Returns the identifier unchanged when valid.
    """
    if not name or not _IDENT_RE.match(name):
        raise ValueError(f"unsafe SQL identifier fragment: {name!r}")
    return name


def qualified_serving_table(schema: str = SERVING_SCHEMA, table: str = SERVING_TABLE) -> str:
    """Return the fully-qualified serving table name, e.g. ``serving.gold_trajectory_summary``."""
    return f"{_safe_ident(schema)}.{_safe_ident(table)}"


def staging_table_name(run_id: str, table: str = SERVING_TABLE) -> str:
    """Return the run-scoped staging table name (unqualified).

    Pattern: ``<table>__staging_<run_id>``.  ``run_id`` is validated to contain
    only identifier-safe characters.
    """
    return f"{_safe_ident(table)}__staging_{_safe_ident(run_id)}"


def qualified_staging_table(
    run_id: str, schema: str = SERVING_SCHEMA, table: str = SERVING_TABLE
) -> str:
    """Return the fully-qualified run-scoped staging table name."""
    return f"{_safe_ident(schema)}.{staging_table_name(run_id, table)}"


# ---------------------------------------------------------------------------
# DDL builders
# ---------------------------------------------------------------------------


def create_schema_sql(schema: str = SERVING_SCHEMA) -> str:
    """DDL: create the serving schema if absent."""
    return f"CREATE SCHEMA IF NOT EXISTS {_safe_ident(schema)};"


def _columns_ddl() -> str:
    parts = []
    for name, pg_type, not_null in _SERVING_COLUMNS:
        null = " NOT NULL" if not_null else ""
        parts.append(f"    {name} {pg_type}{null}")
    return ",\n".join(parts)


def create_table_sql(qualified_table: str, *, with_pk: bool = True) -> str:
    """DDL: create a serving-shaped table (used for staging and final table).

    When *with_pk* is true a composite PRIMARY KEY on the grain
    ``(source_file, track_id)`` is added — this both enforces uniqueness and
    NOT NULL on the grain, and provides the lookup index.
    """
    pk = ""
    if with_pk:
        pk = f",\n    PRIMARY KEY ({', '.join(SERVING_GRAIN_KEYS)})"
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_table} (\n"
        f"{_columns_ddl()}"
        f"{pk}\n"
        f");"
    )


def create_staging_table_sql(run_id: str, schema: str = SERVING_SCHEMA) -> str:
    """DDL: create the run-scoped staging table.

    Staging carries the same shape and PRIMARY KEY as the final table so grain
    violations fail fast during load, before publish.
    """
    return create_table_sql(qualified_staging_table(run_id, schema), with_pk=True)


def drop_staging_table_sql(run_id: str, schema: str = SERVING_SCHEMA) -> str:
    """DDL: drop the run-scoped staging table (cleanup)."""
    return f"DROP TABLE IF EXISTS {qualified_staging_table(run_id, schema)};"


def insert_columns_csv() -> str:
    """Return the ordered column list for INSERT/COPY statements."""
    return ", ".join(SERVING_COLUMN_NAMES)


# ---------------------------------------------------------------------------
# Validation query set (run against staging before publish, and after publish)
# ---------------------------------------------------------------------------


def validation_queries(qualified_table: str) -> dict[str, str]:
    """Return named validation SQL queries for a serving-shaped table.

    Each query returns a single scalar; the caller compares against the
    expected value.  Kept as data so the loader and the local unit tests share
    the exact same SQL.
    """
    grain = ", ".join(SERVING_GRAIN_KEYS)
    return {
        "row_count": f"SELECT count(*) FROM {qualified_table};",
        "distinct_grain": f"SELECT count(*) FROM (SELECT DISTINCT {grain} FROM {qualified_table}) t;",
        "sum_frame_count": f"SELECT coalesce(sum(frame_count), 0) FROM {qualified_table};",
        "min_frame_count": f"SELECT coalesce(min(frame_count), 0) FROM {qualified_table};",
        "bad_duration": f"SELECT count(*) FROM {qualified_table} WHERE duration_s < 0;",
        "bad_distance": f"SELECT count(*) FROM {qualified_table} WHERE traveled_d_m < 0;",
        "bad_speed": (
            f"SELECT count(*) FROM {qualified_table} "
            f"WHERE min_speed_ms < 0 OR max_speed_ms < 0;"
        ),
        "bad_time_order": f"SELECT count(*) FROM {qualified_table} WHERE start_time_s > end_time_s;",
        "null_grain": (
            f"SELECT count(*) FROM {qualified_table} "
            f"WHERE source_file IS NULL OR track_id IS NULL;"
        ),
    }


def storage_queries(qualified_table: str) -> dict[str, str]:
    """Return PostgreSQL storage-introspection queries for the serving table."""
    return {
        "table_bytes": f"SELECT pg_relation_size('{qualified_table}');",
        "index_bytes": f"SELECT pg_indexes_size('{qualified_table}');",
        "total_bytes": f"SELECT pg_total_relation_size('{qualified_table}');",
        "database_bytes": "SELECT pg_database_size(current_database());",
    }


def spark_to_pg_type(spark_type_name: str) -> str:
    """Map a Spark type name (e.g. ``DoubleType``) to a PostgreSQL type."""
    try:
        return _SPARK_TO_PG[spark_type_name]
    except KeyError as exc:
        raise ValueError(f"no PostgreSQL mapping for Spark type {spark_type_name!r}") from exc
