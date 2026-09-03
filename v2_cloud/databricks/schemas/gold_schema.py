"""Explicit Spark schema for the V2 Gold pNEUMA trajectory-summary table.

This schema is the source of truth for the Gold ``trajectory_summary`` Parquet
output.  It is derived directly from the V1 dbt model
``int_vehicle_trajectory_summary`` (see ``GOLD_CONTRACT.md``) and reproduces the
same 19-column relational contract that ``fct_vehicle_trajectories`` exposes in
V1.

Design mirrors ``silver_schema.py``:
- A PySpark-free ``_FIELD_DEFS`` registry (always importable in local tests).
- Derived name lists for validators.
- A lazy ``get_gold_schema()`` that builds the ``StructType`` only when PySpark
  is available (i.e. on a Databricks runtime).

Grain
-----
One row per ``(source_file, track_id)`` trajectory.  ``track_id`` is not
globally unique across source files, so the composite key is the trajectory
identity.

Type notes
----------
- ``frame_count`` is ``LongType`` (Spark ``count(*)``), mapping cleanly to
  PostgreSQL ``bigint`` for the later Neon serving milestone.
- All metric fields are relational scalars — no nested/complex Spark types.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Field definitions (PySpark-free — always importable)
# ---------------------------------------------------------------------------

# Ordered list of (name, spark_type_name, nullable) tuples.
# Order and set match V1 int_vehicle_trajectory_summary exactly.
_FIELD_DEFS: list[tuple[str, str, bool]] = [
    # Grain
    ("source_file",      "StringType",  False),
    ("track_id",         "IntegerType", False),
    # Categorical
    ("vehicle_type",     "StringType",  False),
    # Counts
    ("frame_count",      "LongType",    False),
    # Time boundaries
    ("start_time_s",     "DoubleType",  False),
    ("end_time_s",       "DoubleType",  False),
    ("duration_s",       "DoubleType",  False),
    # Source-provided track-level metrics
    ("traveled_d_m",     "DoubleType",  False),
    ("avg_speed_ms",     "DoubleType",  False),
    # Frame-derived speed metrics
    ("max_speed_ms",     "DoubleType",  False),
    ("min_speed_ms",     "DoubleType",  False),
    # Acceleration averages and extremes
    ("avg_lon_acc_ms2",  "DoubleType",  False),
    ("avg_lat_acc_ms2",  "DoubleType",  False),
    ("max_lon_acc_ms2",  "DoubleType",  False),
    ("max_lat_acc_ms2",  "DoubleType",  False),
    # Start/end coordinates (by time)
    ("start_lat",        "DoubleType",  False),
    ("start_lon",        "DoubleType",  False),
    ("end_lat",          "DoubleType",  False),
    ("end_lon",          "DoubleType",  False),
]

# Ordered field names — importable without PySpark.
GOLD_FIELD_NAMES: list[str] = [name for name, _, _ in _FIELD_DEFS]

# Non-nullable field names — used by validator null checks (all fields here).
GOLD_NON_NULLABLE_FIELDS: list[str] = [
    name for name, _, nullable in _FIELD_DEFS if not nullable
]

# Composite grain key.
GOLD_GRAIN_KEYS: list[str] = ["source_file", "track_id"]

# Rounding applied to frame columns before aggregation, mirroring V1 staging
# (stg_vehicle_trajectories).  Coordinates 6 d.p.; kinematics 4 d.p.
COORD_ROUND_DP: int = 6
KINEMATIC_ROUND_DP: int = 4

# Float tolerance for V1/V2 parity comparison of floating-point aggregates.
# Integer/count and categorical fields are compared exactly.  See GOLD_CONTRACT.md §7.
FLOAT_TOLERANCE: float = 1e-6

# Float-valued Gold columns (subject to FLOAT_TOLERANCE in parity checks).
GOLD_FLOAT_FIELDS: list[str] = [
    name for name, type_name, _ in _FIELD_DEFS if type_name == "DoubleType"
]

# Exact-match Gold columns (categorical + counts).
GOLD_EXACT_FIELDS: list[str] = [
    name for name, type_name, _ in _FIELD_DEFS
    if type_name in ("StringType", "IntegerType", "LongType")
]


# ---------------------------------------------------------------------------
# Spark schema (lazy — requires PySpark at call time)
# ---------------------------------------------------------------------------

def get_gold_schema():  # -> pyspark.sql.types.StructType
    """Build and return the Gold StructType schema.

    Called lazily so the module can be imported without PySpark installed.
    Raises ``ModuleNotFoundError`` if PySpark is unavailable.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    _type_map = {
        "StringType":  StringType(),
        "IntegerType": IntegerType(),
        "LongType":    LongType(),
        "DoubleType":  DoubleType(),
    }

    return StructType([
        StructField(name, _type_map[type_name], nullable=nullable)
        for name, type_name, nullable in _FIELD_DEFS
    ])


# Convenience proxy mirroring SILVER_SCHEMA — builds the StructType on first use.
class _LazyGoldSchema:
    """Proxy that builds the StructType on first attribute/index access."""

    _instance = None

    def _get(self):
        if self._instance is None:
            self._instance = get_gold_schema()
        return self._instance

    @property
    def fields(self):
        return self._get().fields

    def __repr__(self):
        return repr(self._get())

    def __eq__(self, other):
        return self._get() == other

    def __iter__(self):
        return iter(self._get())


GOLD_SCHEMA = _LazyGoldSchema()
