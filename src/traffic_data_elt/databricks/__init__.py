"""Databricks/Spark runtime modules for the V2 cloud pipeline.

These modules are packaged in the ``traffic-data-elt`` wheel so Databricks
serverless jobs import them from a versioned artifact
(``traffic_data_elt.databricks.*``) rather than from source files synced onto
``sys.path``.  Orchestration-only assets (notebooks, job definitions, one-time
setup SQL) intentionally remain under ``v2_cloud/databricks/`` and are not part
of the installable package.

Import policy
-------------
PySpark and psycopg are imported lazily inside functions, so these modules
import cleanly in local unit tests without a Spark or PostgreSQL install.
"""
