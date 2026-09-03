"""Explicit Spark schema for the V2 Silver pNEUMA trajectories table.

The schema is the source of truth for Silver Parquet output.  It must align
with the fields produced by ``PneumaRecord`` and ``PneumaExtractor``.

Defining it here (rather than inferring it from the DataFrame) ensures:
- Consistent column types across all Silver writes.
- Early failure when the parser output diverges from the expected contract.
- A single import point for the schema used in both the writer and validator.

Schema design
-------------
All 11 ``PneumaRecord`` fields are preserved exactly.  Two provenance columns
are appended:

``bronze_key``
    The S3 Bronze object key the ZIP was downloaded from.  Enables traceability
    back to the immutable source archive.

``ingested_at``
    UTC timestamp of the Silver write.  Supports incremental refresh and
    audit queries.

No partition columns are added at this layer; partitioning strategy is deferred
to the production pipeline milestone.

PySpark import strategy
-----------------------
``pyspark`` is only available inside a Databricks cluster runtime.  All Spark
types are therefore imported lazily inside ``get_silver_schema()`` so that the
rest of this module (constants, field name lists) can be imported in local unit
tests without a PySpark installation.

Use :func:`get_silver_schema` at runtime inside Spark code.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Field definitions (PySpark-free — always importable)
# ---------------------------------------------------------------------------

# Ordered list of (name, spark_type_name, nullable) tuples.
# This is the authoritative field registry; SILVER_SCHEMA is derived from it.
_FIELD_DEFS: list[tuple[str, str, bool]] = [
    # Parser fields (from PneumaRecord)
    ("source_file",   "StringType",    False),
    ("track_id",      "IntegerType",   False),
    ("vehicle_type",  "StringType",    False),
    ("traveled_d_m",  "DoubleType",    False),
    ("avg_speed_ms",  "DoubleType",    False),
    ("lat",           "DoubleType",    False),
    ("lon",           "DoubleType",    False),
    ("speed_ms",      "DoubleType",    False),
    ("lon_acc_ms2",   "DoubleType",    False),
    ("lat_acc_ms2",   "DoubleType",    False),
    ("timestamp_s",   "DoubleType",    False),
    # Provenance columns
    ("bronze_key",    "StringType",    False),
    ("ingested_at",   "TimestampType", False),
]

# Ordered field names — importable without PySpark.
SILVER_FIELD_NAMES: list[str] = [name for name, _, _ in _FIELD_DEFS]

# Non-nullable field names — used by validator null checks.
SILVER_NON_NULLABLE_FIELDS: list[str] = [
    name for name, _, nullable in _FIELD_DEFS if not nullable
]

# Coordinate and measurement bounds — mirrored from the shared parser so the
# Silver validator can enforce the same constraints at the DataFrame level.
LAT_MIN: float = 37.9
LAT_MAX: float = 38.1
LON_MIN: float = 23.6
LON_MAX: float = 23.9
SPEED_MIN: float = 0.0
SPEED_MAX: float = 200.0  # m/s


# ---------------------------------------------------------------------------
# Spark schema (lazy — requires PySpark at call time)
# ---------------------------------------------------------------------------

def get_silver_schema():  # -> pyspark.sql.types.StructType
    """Build and return the Silver StructType schema.

    Called lazily so the module can be imported without PySpark installed.
    Raises ``ModuleNotFoundError`` if PySpark is unavailable.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    _type_map = {
        "StringType":    StringType(),
        "IntegerType":   IntegerType(),
        "DoubleType":    DoubleType(),
        "TimestampType": TimestampType(),
    }

    return StructType([
        StructField(name, _type_map[type_name], nullable=nullable)
        for name, type_name, nullable in _FIELD_DEFS
    ])


# Convenience alias used by Spark code: ``from ...silver_schema import SILVER_SCHEMA``
# This is a property-like deferred object — access triggers the lazy build.
# For code that needs to import it at module level inside Databricks, call
# ``get_silver_schema()`` once and assign to a local variable.
class _LazySilverSchema:
    """Proxy that builds the StructType on first attribute/index access."""

    _instance = None

    def _get(self):
        if self._instance is None:
            self._instance = get_silver_schema()
        return self._instance

    # Delegate the most common StructType operations used downstream.
    @property
    def fields(self):
        return self._get().fields

    def __repr__(self):
        return repr(self._get())

    def __eq__(self, other):
        return self._get() == other

    def __iter__(self):
        return iter(self._get())


SILVER_SCHEMA = _LazySilverSchema()
