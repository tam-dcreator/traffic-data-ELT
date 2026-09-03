# V2 — Databricks / Spark Processing

This directory contains the Databricks/Spark processing layer for V2.

Two stages are implemented:

- **Silver** — reads S3 Bronze, runs the shared pNEUMA parser, writes S3 Silver
  (normalised frame-level Parquet).
- **Gold** — reads S3 Silver, aggregates to the trajectory level (the Spark
  implementation of the V1 dbt `int_vehicle_trajectory_summary`), writes S3 Gold
  (`trajectory_summary`).

## Transformation boundary

```
Silver   → normalised frame-level canonical data (one row per frame)
Gold     → trajectory-level computational aggregates (one row per trajectory)
Neon/dbt → later serving + semantic layer (business naming, marts, tests, docs)
```

Gold performs the heavy trajectory aggregation that V1 previously did in
PostgreSQL/dbt. The business-semantic dbt layer (dimensional models, KPI naming,
lineage, documentation) is **not** moved into Gold — it stays a dbt
responsibility after Gold-derived data reaches Neon in a later milestone.

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
├── GOLD_CONTRACT.md           ← authoritative Gold trajectory_summary contract
├── bronze_reader.py           ← S3 ZIP download, UC volume extraction, cleanup
├── silver_writer.py           ← PneumaRecord → Spark DataFrame → S3 Parquet
├── silver_validator.py        ← strict Silver post-write validation
├── gold_transformer.py        ← Silver frames → trajectory_summary → S3 Gold
├── gold_validator.py          ← strict Gold post-write validation (14 checks)
├── gold_parity.py             ← pure-Python V1/V2 field-level parity comparison
├── schemas/
│   ├── silver_schema.py       ← explicit Silver StructType + validation bounds
│   └── gold_schema.py         ← explicit Gold StructType + rounding/tolerance
├── notebooks/
│   ├── silver_pipeline.py     ← Silver orchestration notebook (cell-by-cell)
│   └── gold_pipeline.py       ← Gold orchestration notebook (cell-by-cell)
├── jobs/
│   ├── silver_job.yml         ← Silver Databricks Bundles job definition
│   └── gold_job.yml           ← Gold Databricks Bundles job definition
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
| `S3_GOLD_PREFIX` | `gold` | S3 Gold key prefix |
| `UC_CATALOG` | `workspace` | Unity Catalog catalog name |
| `UC_SCHEMA` | `default` | Unity Catalog schema name |
| `UC_VOLUME` | `v2_temp` | Unity Catalog volume name |
| `DATABRICKS_HOST` | — | Workspace URL (override CLI profile) |

Secrets (AWS credentials, Databricks token if needed) must use Databricks
Secrets or the CLI OAuth profile — never `.env`.

---

## Testing

```bash
# Unit tests (no AWS, no Spark, no PySpark required)
pytest tests/unit/test_silver_schema.py
pytest tests/unit/test_bronze_reader.py
pytest tests/unit/test_silver_writer.py
pytest tests/unit/test_silver_validator.py
pytest tests/unit/test_gold_schema.py
pytest tests/unit/test_gold_transformer.py
pytest tests/unit/test_gold_validator.py
pytest tests/unit/test_gold_parity.py

# Silver integration test (requires live AWS + Spark)
pytest tests/integration/test_silver_databricks.py -v -s

# Gold V1/V2 parity test (requires the V1 warehouse + a Gold export JSON)
TRAFFIC_DB_NAME=... TRAFFIC_DB_USER=... TRAFFIC_DB_PASSWORD=... \
GOLD_EXPORT_JSON=/path/to/gold_export.json \
pytest tests/integration/test_gold_v1_parity.py -v -s
```

Integration tests are auto-skipped when their required resources
(`AWS_REGION`/`S3_BUCKET`, PySpark, the V1 warehouse env vars, or the Gold
export file) are absent. The unit suite never depends on live Databricks, AWS,
S3, or a local PySpark install.

---

## Gold — trajectory_summary

The first Gold dataset is the trajectory-level `trajectory_summary`, the Spark
implementation of the V1 dbt `intermediate.int_vehicle_trajectory_summary`.
The full contract (grain, 19-column V1→Spark mapping, rounding, invariants,
float tolerance) is in `GOLD_CONTRACT.md`.

### Flow

```
S3 Silver frame Parquet
    ↓  spark.read.parquet  (UC external location — no boto3, no AWS keys)
    ↓  build_trajectory_summary(...)   — native Spark aggregations only
    ↓  S3 Gold Parquet  (gold/pneuma/trajectory_summary/test/)
    ↓  read-back
    ↓  validate_gold(...)  — strict on real Spark
```

Silver → Spark → Gold is a **direct S3-to-S3** transformation. No UC volume
extraction or temporary data layer is used for Gold.

### Grain and semantics

- Grain: one row per `(source_file, track_id)`. `track_id` is not globally
  unique across source files, so the composite key is the trajectory identity.
- Frame columns are rounded to match V1 staging (coords 6 d.p., kinematics
  4 d.p.) **before** aggregation, so Gold reproduces V1 field-for-field.
- Aggregations use native Spark functions (`groupBy`, `count`, `min`, `max`,
  `avg`, `first` over an ordered window). No Python UDFs.

### Invariants validated (strict, on the persisted Parquet)

1. Schema field names + types match the Gold contract.
2. No nulls in any of the 19 columns.
3. Trajectory key `(source_file, track_id)` is unique.
4. `SUM(frame_count) == COUNT(silver rows)` — frame conservation.
5. `frame_count >= 1`, `start_time_s <= end_time_s`, `duration_s >= 0`,
   `traveled_d_m >= 0`, `min/max speed >= 0`.
6. Read-back row count matches the write count.

### V1/V2 parity

Field-level parity against the V1 dbt `int_vehicle_trajectory_summary` (same
sample) is verified off-cluster by `tests/integration/test_gold_v1_parity.py`
using `gold_parity.compare_trajectory_summaries`. Integer/categorical fields are
compared exactly; floating-point aggregates use a `1e-6` absolute tolerance
(see `GOLD_CONTRACT.md` §7).

| Metric | Expected (sample) |
|---|---|
| Silver frame rows | 1,446,887 |
| Gold trajectories | 922 |
| SUM(frame_count) | 1,446,887 |
| Field mismatches vs V1 | 0 |

### Running the Gold notebook

Same serverless mechanism as Silver. Sync the `v2_cloud/databricks/*` modules to
the UC volume `code/` path, import `notebooks/gold_pipeline.py` into the
workspace, and run on Serverless (via the notebook UI or `databricks jobs
submit` with a serverless environment). The wheel from the Silver setup is
reused — the Gold runtime path is pure standard library.

---

## Code deployment — temporary development mechanism (technical debt)

The `v2_cloud/databricks/*` modules live outside `src/` and are therefore **not**
part of the `traffic-data-elt` wheel. For active development they are synced to
a Unity Catalog volume `code/` path and added to `sys.path` at notebook startup:

```
v2_cloud/databricks/*.py
    →  /Volumes/workspace/default/v2_temp/code/v2_cloud/databricks/
    →  sys.path.insert(0, "/Volumes/workspace/default/v2_temp/code")
```

This is a **temporary development sync**, not the production packaging story.
Both the Silver and Gold notebooks use it.

```
current development code sync   →  temporary UC-volume sync + sys.path
future production packaging     →  separate milestone (wheel or bundle-managed)
```

A future milestone should package these modules (wheel or Databricks Asset
Bundle file sync) so no manual UC-volume sync is required.

---

## What is not in scope for this milestone

- Additional Gold datasets: `traffic_metrics`, `vehicle_type_metrics`, time
  aggregates, area aggregates (this branch proves `trajectory_summary` only)
- Neon / Cloud PostgreSQL loading
- V2 dbt models / target changes
- Full V2 Airflow orchestration DAG
- Full production archive processing (~15 GB)
- Production code packaging/deployment redesign
- Classic Databricks clusters / `DATABRICKS_CLUSTER_ID`
