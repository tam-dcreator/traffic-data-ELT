# Project

## Name

traffic_data_ELT

## Goal

Build a Data Engineering portfolio project that demonstrates architectural scaling through two implementations:

- V1 — Local Prototype
- V2 — Cloud Scale

## V1

Local Docker-based ELT platform using:

- Airflow
- PostgreSQL
- dbt Core
- Redash
- pNEUMA sample data

## V2

Cloud-scale version using:

- Local Airflow
- AWS S3
- Databricks / Spark
- Cloud PostgreSQL
- the same dbt project

## Engineering Principles

- Keep V1 and V2 in one monorepo.
- Reuse transformation and business logic where possible.
- Keep Airflow DAGs thin.
- Put reusable Python logic under `src/traffic_data_elt/`.
- Keep secrets out of source control.
- Prefer lightweight containers.
- Avoid unnecessary dependencies.
- Preserve reproducibility across systems.
