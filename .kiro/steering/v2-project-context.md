# Traffic Data ELT — V2 Cloud Scale Project Context

_Last updated: 2026-08-30_

## 1. V2 Goal

V2 demonstrates how the Traffic Data ELT platform evolves from the completed V1 local prototype into a cloud-scale architecture capable of processing the full multi-gigabyte pNEUMA dataset.

The architectural objective is to scale storage and compute while preserving reusable parsing logic, orchestration principles, data-quality controls, business semantics, and the shared dbt project.

V2 is an evolution of V1, not a separate unrelated system.

---

## 2. V1 → V2 Evolution

### V1

```text
pNEUMA sample CSV
        ↓
Airflow
        ↓
Python parser / RawLoader
        ↓
PostgreSQL raw
        ↓
dbt staging
        ↓
dbt intermediate
        ↓
dbt marts
        ↓
Redash
```

### V2

```text
Full pNEUMA archive / Zenodo
        ↓
Local Airflow
        ↓
Amazon S3 Bronze
        ↓
Databricks / Spark
        ↓
Shared Python pNEUMA parser
        ↓
Amazon S3 Silver
        ↓
Spark transformations
        ↓
Amazon S3 Gold
        ↓
Neon PostgreSQL
        ↓
Shared dbt marts
        ↓
Analytics / BI
```

---

## 3. V2 Technology Stack

### Orchestration

- Apache Airflow 3.x
- Runs locally in the existing V2 runtime environment
- Responsible for orchestration, retries, failure handling, monitoring, and dependency control
- DAGs remain thin
- Heavy data processing is delegated to Databricks

### Data Lake

- Amazon S3
- One bucket
- Medallion architecture implemented with prefixes:
  - `bronze/`
  - `silver/`
  - `gold/`

### Distributed Processing

- Databricks
- Apache Spark
- Responsible for:
  - archive decompression
  - source parsing at scale
  - normalization
  - large-scale transformations
  - aggregation
  - Parquet generation

### Serving Warehouse

- Neon PostgreSQL
- Stores compact Gold-derived serving datasets only
- Frame-level Silver data does not go to Neon

### Semantic / Transformation Layer

- dbt Core
- Existing shared project:

```text
dbt/traffic_dwh/
```

No separate V2 dbt project should be created.

---

## 4. Complete V2 Data Flow

```text
Zenodo
  │
  │ HTTP stream
  ▼
Local Airflow
  │
  │ stream upload; no 15 GB local staging
  ▼
Amazon S3 Bronze
  │
  │ immutable compressed source archive
  ▼
Databricks
  │
  │ download to ephemeral storage
  │ unzip archive
  ▼
Individual pNEUMA CSV files
  │
  ▼
Shared Python pNEUMA parser
  │
  │ reconstruct logical vehicle records
  │ repair known source splits
  │ validate 4 + N×6 structure
  ▼
Spark DataFrame
  │
  ▼
Amazon S3 Silver
  │
  │ normalized frame-level Parquet
  ▼
Databricks / Spark
  │
  │ aggregate, enrich, transform
  ▼
Amazon S3 Gold
  │
  │ curated analytical Parquet
  ▼
Neon PostgreSQL
  │
  │ compact serving datasets
  ▼
Shared dbt project
  │
  │ semantic marts, tests, docs
  ▼
Analytics / BI
```

---

## 5. S3 Data Lake Design

Use one S3 bucket rather than separate buckets for Bronze, Silver, and Gold.

Example:

```text
s3://traffic-data-v2/
│
├── bronze/
│   └── pneuma/
│       └── pneuma.zip
│
├── silver/
│   └── pneuma/
│       └── trajectories/
│           └── *.parquet
│
└── gold/
    └── pneuma/
        ├── trajectory_summary/
        ├── traffic_metrics/
        └── vehicle_type_metrics/
```

Different storage, lifecycle, and IAM policies can be applied by prefix.

---

## 6. Bronze Layer

### Purpose

Bronze preserves the immutable original source dataset.

Expected object:

```text
bronze/pneuma/pneuma.zip
```

The approximately 15 GB source archive is streamed directly from Zenodo to S3. The complete archive should not be staged on the local development machine.

### Storage Class

Initial recommendation:

```text
S3 Intelligent-Tiering
```

Bronze remains immediately retrievable during development because parser changes, failed processing, or pipeline reconstruction may require reprocessing.

After the pipeline stabilizes, lifecycle policies can transition inactive Bronze objects to Glacier-class archival storage.

---

## 7. Archive Handling

Airflow does not decompress the archive.

```text
Zenodo ZIP
    ↓
stream unchanged
    ↓
S3 Bronze
    ↓
Databricks
    ↓
ephemeral storage
    ↓
unzip
```

This keeps local Airflow lightweight and moves decompression to the cloud compute layer.

The extracted CSVs do not need to be permanently duplicated in S3. They can exist temporarily on Databricks ephemeral storage, be parsed, and then be discarded after Silver is written successfully.

---

## 8. Shared pNEUMA Parsing Strategy

The V1 parser contains source-specific logic that should be reused in V2 rather than replaced by a generic Spark CSV reader.

The parser must support:

- logical vehicle-record reconstruction
- repeated trajectory-frame structure
- contextual repair of known numeric/source splits
- field-position preservation
- malformed record rejection
- frame-level normalization

Logical record structure:

```text
4 vehicle fields
+
N × 6 frame fields
```

Frame structure:

```text
latitude
longitude
speed
longitudinal acceleration
lateral acceleration
timestamp
```

### V1

```text
Local file
    ↓
Python parser
    ↓
Python records
    ↓
psycopg
    ↓
PostgreSQL
```

### V2

```text
Databricks-local CSV
    ↓
Same parsing rules
    ↓
Spark rows
    ↓
Spark DataFrame
    ↓
S3 Silver Parquet
```

The parser should be refactored so the core parsing logic accepts a stream/file-like source instead of being tightly coupled to a local `Path`.

---

## 9. Spark Parsing Strategy

Parallelism should occur primarily across individual source CSV files rather than arbitrary byte ranges inside a pNEUMA CSV.

```text
CSV 001 → Spark task → Python parser
CSV 002 → Spark task → Python parser
CSV 003 → Spark task → Python parser
...
```

This avoids creating Spark partition boundaries inside malformed or reconstructed logical vehicle records.

---

## 10. Silver Layer

Silver is the canonical normalized frame-level trajectory dataset.

Format:

```text
Parquet
```

Benefits:

- columnar storage
- compression
- efficient Spark scans
- predicate pushdown
- no repeated CSV reparsing
- reduced storage and compute overhead for downstream work

Example layout:

```text
silver/pneuma/trajectories/
    ├── date=.../
    │   ├── part-001.parquet
    │   ├── part-002.parquet
    │   └── ...
```

Recommended target object size:

```text
128–512 MB
```

Practical target:

```text
~256 MB per Parquet object
```

Avoid high-cardinality partitioning such as `track_id`.

### Storage Class

```text
S3 Standard
```

Silver is expected to be actively scanned during development and transformation work.

---

## 11. Gold Layer

Gold contains curated analytical datasets produced by Spark.

Examples:

```text
trajectory_summary
traffic_metrics
vehicle_type_metrics
time-based traffic aggregates
area-based traffic aggregates
```

Gold should remove the need for common analytical workloads to rescan frame-level Silver data.

### Storage Class

```text
S3 Standard
```

Gold should remain immediately accessible while it is feeding Neon and downstream analytical workloads.

---

## 12. V1 dbt vs V2 Spark Responsibilities

### V1

```text
PostgreSQL raw
    ↓
dbt staging
    ↓
dbt intermediate
    ↓
dbt marts
```

### V2

```text
S3 Bronze
    ↓
Spark parsing
    ↓
S3 Silver
    ↓
Spark heavy transformations
    ↓
S3 Gold
    ↓
Neon
    ↓
dbt semantic marts
```

V1 dbt transformations that perform frame-level cleanup or trajectory-level aggregation should move into Spark for V2.

The logical responsibility of models such as:

```text
int_vehicle_trajectory_summary
```

becomes part of the Spark Gold pipeline.

The shared dbt project remains, but V2 does not need to execute the same complete `staging+` chain used by V1.

---

## 13. dbt Role in V2

Spark handles:

- heavy parsing
- frame-level transformations
- large aggregations
- distributed joins
- creation of Gold datasets

DBT handles:

- dimensional modeling
- business naming
- KPI definitions
- lightweight serving transformations
- data-quality tests
- lineage
- documentation
- BI-facing views and marts

Existing marts such as:

```text
fct_vehicle_trajectories
dim_vehicle_type
```

can remain useful.

Do not duplicate Spark transformation logic inside dbt.

---

## 14. Neon PostgreSQL Design

Neon acts as the serving warehouse rather than the primary V2 storage layer.

Do not load Silver/frame-level trajectory data into Neon.

```text
S3 Gold
    ↓
selected compact datasets
    ↓
Neon PostgreSQL
    ↓
dbt semantic models
```

Likely physical serving base table:

```text
gold_trajectory_summary
```

DBT can expose models such as:

```text
fct_vehicle_trajectories
dim_vehicle_type
analytics_* models
```

Large fact-style dbt models should preferably be views over physical Gold-derived serving tables when materializing another full copy would unnecessarily duplicate storage.

### Storage Target

Design for approximately:

```text
≤ 300–350 MB total Neon usage
```

This preserves headroom beneath a 500 MB project-storage ceiling for indexes, PostgreSQL overhead, dead tuples, and future models.

---

## 15. Storage Responsibility

```text
S3 Bronze
→ immutable compressed source

S3 Silver
→ normalized detailed trajectory data

S3 Gold
→ curated analytical datasets

Neon PostgreSQL
→ compact serving datasets

dbt
→ semantic/business layer
```

The heavy trajectory dataset remains in S3.

---

## 16. Cost Strategy

### S3

- one bucket rather than multiple buckets
- one retained compressed source archive in Bronze
- Intelligent-Tiering for Bronze during active development
- lifecycle Bronze to Glacier later
- Standard for active Silver and Gold data
- use Parquet compression
- avoid small-file proliferation
- prefer ~256 MB Parquet objects
- use coarse, query-relevant partitions

### Databricks

- use distributed compute only for workloads that benefit from it
- decompress the source archive in Databricks, not local Airflow
- persist Silver so the raw archive is parsed once
- persist Gold so common analytics do not repeatedly scan Silver

### Neon

- load only compact serving data
- avoid frame-level datasets
- avoid duplicate physical dbt fact tables
- prefer views where practical

---

## 17. Airflow V2 Responsibilities

Expected logical DAG:

```text
stream_zenodo_to_s3
        ↓
validate_bronze
        ↓
trigger_databricks_parse
        ↓
validate_silver
        ↓
trigger_databricks_transform
        ↓
validate_gold
        ↓
load_gold_to_neon
        ↓
dbt_run
        ↓
dbt_test
        ↓
pipeline_success
```

Airflow should not:

- parse the full dataset itself
- perform Spark-scale transformations
- contain large business-logic implementations
- persist the full archive locally

Reusable Python orchestration and ingestion logic should remain under:

```text
src/traffic_data_elt/
```

---

## 18. Proposed Shared Python Structure

```text
src/traffic_data_elt/
├── config/
├── extract/
│   ├── pneuma.py
│   └── zenodo.py
├── load/
│   ├── raw_loader.py
│   └── s3_uploader.py
├── transform/
└── utils/
```

Responsibilities:

```text
pneuma.py
→ shared source-format parsing rules

zenodo.py
→ Zenodo streaming/download logic

raw_loader.py
→ V1 PostgreSQL raw loading

s3_uploader.py
→ V2 S3 Bronze ingestion
```

---

## 19. V2 Repository Scope

Version-specific infrastructure remains under:

```text
v2_cloud/
```

Target structure:

```text
v2_cloud/
├── airflow/
│   ├── dags/
│   └── config/
│
├── aws/
│   ├── s3/
│   ├── iam/
│   └── terraform/
│
├── databricks/
│   ├── notebooks/
│   ├── jobs/
│   └── schemas/
│
└── postgres/
    └── config/
```

Shared components remain outside `v2_cloud/`:

```text
src/traffic_data_elt/
dbt/traffic_dwh/
tests/
docs/
```

---

## 20. Core V2 Architectural Principle

```text
Airflow
→ orchestration

S3
→ durable lake storage

Python
→ pNEUMA source-format intelligence

Databricks / Spark
→ distributed parsing and heavy transformation

Parquet
→ efficient analytical storage

Neon PostgreSQL
→ compact serving warehouse

dbt
→ semantic models, testing, lineage and documentation

BI
→ visualization and analysis
```

The architectural evolution is:

```text
V1
PostgreSQL + dbt perform most warehouse transformations

                    ↓ scales into

V2
S3 + Spark perform large-scale storage and processing
while PostgreSQL + dbt become the lightweight serving/semantic layer
```

V2 preserves the engineering principles established in V1: thin Airflow DAGs, reusable Python logic, one shared dbt project, explicit data-quality controls, observability, reproducibility, idempotency, and clear separation of responsibilities.
