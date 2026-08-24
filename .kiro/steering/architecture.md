# Architecture

## Repository Model

Use a single monorepo for both versions of the platform.

## Shared Components

Shared logic belongs outside version-specific infrastructure folders.

Use:

- `src/traffic_data_elt/` for reusable Python logic
- `dbt/traffic_dwh/` for the single shared dbt project
- `tests/` for shared test suites
- `docs/` for architecture and operational documentation

## V1 — Local Prototype

V1 runs locally with Docker.

Flow:

```text
pNEUMA sample CSV
        |
        v
     Airflow
        |
        v
   PostgreSQL
        ^
        |
      dbt
        |
        v
     Redash
```

Version-specific infrastructure belongs under:

```text
v1_local/
```

## V2 — Cloud Scale

V2 uses cloud storage and distributed processing.

Flow:

```text
Zenodo
   |
   v
Local Airflow
   |
   v
AWS S3
   |
   v
Databricks / Spark
   |
   v
Cloud PostgreSQL
   ^
   |
  dbt
```

Version-specific infrastructure belongs under:

```text
v2_cloud/
```

## Airflow Design

Airflow DAGs should remain thin.

DAGs should orchestrate reusable functions rather than contain large amounts of business logic.

Reusable logic should be imported from:

```text
src/traffic_data_elt/
```

## dbt Design

Maintain one dbt project for both V1 and V2.

Do not duplicate dbt models between environments.

Environment-specific database targets should be handled through configuration.

## Warehouse Layers

Use logical data layers such as:

- raw
- staging
- intermediate
- marts
- analytics
- audit

Do not use `dev`, `staging`, and `prod` as substitutes for data-layer schemas.

## Container Design

Prefer small, single-purpose containers.

Do not install Airflow into the Dev Container.

Do not place PostgreSQL, Redash, dbt, and Airflow into one container.

The Dev Container is the engineering workspace.

Runtime services must remain separate.

## Development Environment

The project runs from:

```text
Windows
  -> WSL2
      -> Kiro
          -> Dev Container
              -> Docker-in-Docker
                  -> project runtime containers
```

Docker-in-Docker is intentional and must not be replaced with host Docker socket mounting without an explicit architectural decision.
