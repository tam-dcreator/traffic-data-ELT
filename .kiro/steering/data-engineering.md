# Data Engineering

## General Principles

Design for correctness, observability, reproducibility, and clear separation of concerns.

Prefer simple implementations in V1 that can scale conceptually into V2.

## Extraction and Loading

Keep source extraction and warehouse loading logic reusable.

Place shared logic under:

```text
src/traffic_data_elt/
```

Recommended package boundaries:

```text
extract/
load/
transform/
config/
utils/
```

Airflow DAGs should orchestrate this logic rather than implement it directly.

## Data Layers

Use the following warehouse layers:

```text
raw
staging
intermediate
marts
analytics
audit
```

### raw

Store source data with minimal transformation.

Preserve source values where practical.

### staging

Clean, cast, rename, and standardize raw fields.

### intermediate

Hold reusable transformation logic that should not be exposed directly to reporting.

### marts

Create business-oriented dimensional or analytical models.

### analytics

Expose reporting-ready datasets used by dashboards and analysis.

### audit

Store operational metadata such as load status, row counts, execution timestamps, and data quality results.

## dbt

Use one shared dbt project:

```text
dbt/traffic_dwh/
```

Prefer:

- source definitions for raw tables
- staging models as views where appropriate
- intermediate models for reusable transformations
- marts for business models
- tests for data quality
- macros for reusable SQL logic
- documentation for important models and columns

Avoid duplicating SQL across models.

## Data Quality

Data quality checks should cover relevant cases such as:

- null constraints
- uniqueness
- accepted values
- referential integrity
- row-count anomalies
- malformed records
- invalid timestamps
- impossible coordinates or measurements

Critical failures should stop downstream processing.

## Idempotency

Pipeline tasks should be safe to rerun where practical.

Avoid duplicate ingestion when a DAG or task is retried.

Prefer deterministic loads and transformations.

## Schema Changes

Do not silently absorb unexpected source schema changes.

Schema changes should be detected and handled explicitly.

## Observability

Capture useful operational metadata including:

- pipeline run identifier
- source file
- ingestion timestamp
- record counts
- rejected record counts
- task status

Logging should be informative without exposing secrets.

## V1

Optimize V1 for clarity and local reproducibility.

Use PostgreSQL as the warehouse and dbt Core for transformations.

## V2

Optimize V2 for larger datasets and distributed processing.

Stream source data to S3 rather than storing the full dataset locally.

Use Spark / Databricks for large-scale processing while preserving reusable business logic where practical.
