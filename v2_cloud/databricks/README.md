# V2 — Databricks / Spark Processing

This directory contains the Databricks/Spark processing layer for V2.
It reads from S3 Bronze, runs the shared pNEUMA parser, and writes S3 Silver.

---

## Architecture

```
S3 Bronze
└── bronze/pneuma/test/pnemas-sample.zip   ← immutable compressed source
        │
        │  download_and_extract()
        ▼
UC Managed Volume (temporary only)
/Volumes/<catalog>/<schema>/v2_temp/runs/<run_id>/
├── source/pnemas-sample.zip               ← temporary copy
└── extracted/pnemas.csv                   ← temporary extracted CSV
        │
        │  PneumaExtractor.extract_from_lines(...)
        ▼
Spark DataFrame  (explicit Silver schema)
        │
        │  write_silver()
        ▼
S3 Silver
└── silver/pneuma/trajectories/test/       ← normalised Parquet (persistent)
        │
        │  validate_silver()  — strict on real Spark
        ▼
Cleanup /Volumes/.../runs/<run_id>/        ← deleted after validation passes
```

**Durable state after a successful run:**

```
S3 Bronze  →  immutable compressed source ZIP
S3 Silver  →  normalised Parquet
```

The Unity Catalog volume is temporary working storage only.
Extracted CSVs and ZIP copies are removed after successful Silver validation.

---

## Directory layout

```
v2_cloud/databricks/
├── README.md                  ← this file
├── bronze_reader.py           ← S3 ZIP download, UC volume extraction, cleanup
├── silver_writer.py           ← PneumaRecord → Spark DataFrame → S3 Parquet
├── silver_validator.py        ← strict post-write validation (9 checks)
├── schemas/
│   └── silver_schema.py       ← explicit Spark StructType + validation bounds
├── notebooks/
│   └── silver_pipeline.py     ← orchestration notebook (run cell-by-cell)
├── jobs/
│   └── silver_job.yml         ← Databricks Bundles job definition
└── setup/
    └── create_uc_volume.sql   ← one-time Unity Catalog volume setup
```

---

## Authentication

This project uses the **Databricks CLI OAuth flow**.
No personal access token is stored in the repository or in `.env` files.

### One-time setup

```bash
# Install the Databricks CLI (if not already present)
brew install databricks/tap/databricks   # macOS
# or see https://docs.databricks.com/dev-tools/cli/install.html

# Log in to your workspace — opens a browser for OAuth
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# Verify authentication
databricks current-user me
```

A profile is written to `~/.databrickscfg`.
The Databricks SDK and `databricks-connect` both read this profile automatically.

### What NOT to do

- Do **not** put Databricks tokens in `.env` files or source code.
- Do **not** commit `~/.databrickscfg`.
- Do **not** use `DATABRICKS_TOKEN` in configuration committed to the repository.

---

## Compute — Databricks Serverless

This V2 implementation uses **Databricks Serverless Compute**.
No classic cluster, no `DATABRICKS_CLUSTER_ID`, no EC2-backed all-purpose cluster.

### Running the notebook

1. Open `notebooks/silver_pipeline.py` in the Databricks workspace via Repos.
2. Select **Serverless** as the compute target.
3. Run cells in order.

### Local Spark via databricks-connect (optional)

Serverless compute requires `databricks-connect >= 15.1`:

```bash
pip install "databricks-connect>=15.1"
```

Then in Python:

```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().getOrCreate()
```

No cluster ID is required.  The session connects to the workspace
identified by your CLI profile.

> **Trial workspace note:** Serverless availability depends on your
> Databricks plan and region.  If `DatabricksSession.builder.serverless()`
> raises a version compatibility error, run the pipeline directly as a
> Databricks notebook instead — notebooks on serverless compute work
> regardless of the local `databricks-connect` version.

---

## Unity Catalog temporary volume

### Why a UC volume instead of `/tmp`?

Databricks Serverless compute does not provide durable node-local storage
(`/local_disk0` is not available).  A Unity Catalog managed volume provides
a POSIX-compatible path (`/Volumes/...`) accessible from serverless compute
and persists across task retries within the same run.

### One-time setup

Run the SQL in `setup/create_uc_volume.sql` once in a Databricks SQL editor
or notebook cell:

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.v2_temp
  COMMENT 'Temporary V2 Silver pipeline working storage';
```

The volume does not need to be large — the test ZIP is a few MB and the
extracted CSV is ~120 MB.

### Run directory structure

Each pipeline run creates an isolated sub-directory:

```
/Volumes/workspace/default/v2_temp/
└── runs/
    └── <run_id>/
        ├── source/
        │   └── pnemas-sample.zip
        └── extracted/
            └── pnemas.csv
```

`<run_id>` is a short UUID generated at notebook startup.
All files and the run directory are removed after successful Silver validation.

---

## Shared package installation

The `traffic-data-elt` package must be installed on the Databricks runtime
before the notebook can import `traffic_data_elt`.

### Method A — Databricks Repos (recommended for active development)

Attach the repository via Databricks Repos and run in a notebook cell:

```python
%pip install -e /Workspace/Repos/<your-email>/traffic_data_ELT
dbutils.library.restartPython()
```

Changes to `src/traffic_data_elt/` are reflected without rebuilding.

### Method B — Wheel from UC volume

```bash
# Build locally
pip wheel . -w dist/

# Upload wheel to the UC volume using the Databricks CLI
databricks fs cp \
    dist/traffic_data_elt-0.1.0-py3-none-any.whl \
    /Volumes/workspace/default/v2_temp/wheels/traffic_data_elt-0.1.0-py3-none-any.whl
```

Then in the notebook:

```python
%pip install /Volumes/workspace/default/v2_temp/wheels/traffic_data_elt-0.1.0-py3-none-any.whl
dbutils.library.restartPython()
```

---

## Silver schema

Defined in `schemas/silver_schema.py`.

| Column | Spark type | Notes |
|---|---|---|
| `source_file` | StringType | CSV filename from Bronze ZIP |
| `track_id` | IntegerType | Vehicle identifier |
| `vehicle_type` | StringType | Car, Motorcycle, Taxi, Bus, etc. |
| `traveled_d_m` | DoubleType | Total track distance (metres) |
| `avg_speed_ms` | DoubleType | Track-level average speed (m/s) |
| `lat` | DoubleType | Frame latitude — valid: 37.9–38.1 |
| `lon` | DoubleType | Frame longitude — valid: 23.6–23.9 |
| `speed_ms` | DoubleType | Instantaneous speed (m/s) ≥ 0 |
| `lon_acc_ms2` | DoubleType | Longitudinal acceleration (m/s²) |
| `lat_acc_ms2` | DoubleType | Lateral acceleration (m/s²) |
| `timestamp_s` | DoubleType | Elapsed seconds since recording start |
| `bronze_key` | StringType | S3 Bronze object key (provenance) |
| `ingested_at` | TimestampType | UTC write timestamp (provenance) |

Schema is built lazily via `get_silver_schema()` so the module can be
imported without PySpark in local unit tests.

---

## Silver validation

`silver_validator.py` runs 9 checks after every Parquet write:

1. Silver Parquet is readable by Spark
2. Row count > 0
3. V1/V2 parity: row count == 1,446,887 (when `expected_row_count` is supplied)
4. Schema field names match Silver contract
5. All field types match Silver schema — **strict failure on real Spark**
6. No nulls in non-nullable fields
7. Latitude within Athens bounding box [37.9, 38.1]
8. Longitude within Athens bounding box [23.6, 23.9]
9. Speed ≥ 0
10. `source_file` and `bronze_key` populated on all rows

**Local unit tests:** Field type check and data quality checks are skipped
when PySpark is not installed.  A warning is emitted.

**Real Databricks execution:** All checks are strict.  A type mismatch or
null violation is a `failed_check`, not a warning.  Cleanup does not run
until all checks pass.

---

## V1/V2 parity

| Metric | Expected |
|---|---|
| Logical vehicles | 922 |
| Silver frame rows | 1,446,887 |
| Rejected logical records | 0 |

A mismatch stops the milestone.  Diagnose: ZIP integrity, encoding,
newline handling, `extract_from_lines` boundary handling.

---

## Configuration

All configuration uses `v2_cloud/.env` (never committed).

Non-secret values:

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `eu-central-1` | S3 bucket region |
| `S3_BUCKET` | — | S3 bucket name (required) |
| `S3_BRONZE_PREFIX` | `bronze` | S3 Bronze key prefix |
| `S3_SILVER_PREFIX` | `silver` | S3 Silver key prefix |
| `UC_CATALOG` | `workspace` | Unity Catalog catalog name |
| `UC_SCHEMA` | `default` | Unity Catalog schema name |
| `UC_VOLUME` | `v2_temp` | Unity Catalog volume name |
| `DATABRICKS_HOST` | — | Workspace URL (override CLI profile) |

Secrets (AWS credentials, Databricks token if needed) must use Databricks
Secrets or the CLI OAuth profile — never `.env`.

---

## Testing

```bash
# Unit tests (no AWS, no Spark required)
pytest tests/unit/test_silver_schema.py
pytest tests/unit/test_bronze_reader.py
pytest tests/unit/test_silver_writer.py
pytest tests/unit/test_silver_validator.py

# Integration test (requires live AWS + Spark)
pytest tests/integration/test_silver_databricks.py -v -s
```

The integration test is auto-skipped when `AWS_REGION`/`S3_BUCKET` or
PySpark are absent.

---

## What is not in scope for this milestone

- Gold aggregations
- Neon / Cloud PostgreSQL loading
- V2 dbt models
- Full V2 Airflow orchestration DAG
- Full production archive processing (~15 GB)
- Classic Databricks clusters / `DATABRICKS_CLUSTER_ID`
