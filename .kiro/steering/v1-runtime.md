# V1 Runtime Implementation

## Goal

Build the local V1 runtime stack for the Traffic Data ELT project.

The implementation must remain lightweight, reproducible, and suitable for local development inside Docker-in-Docker.

## Services

V1 should provide these systems:

- PostgreSQL
- Apache Airflow
- Redash
- Redis only if required by Redash

A single PostgreSQL server may host separate logical databases for:

- traffic warehouse
- Airflow metadata
- Redash metadata

Do not create unnecessary database containers.

## Docker Compose

Runtime orchestration belongs in:

```text
v1_local/compose.yaml
```

Use:

```text
v1_local/.env
```

for runtime values.

Do not hardcode credentials in Compose files.

Use named Docker volumes for persistent service data.

Use an internal Docker network for service-to-service communication.

Expose only ports required for local access.

## Airflow

Keep Airflow DAGs thin.

DAGs belong under:

```text
v1_local/airflow/dags/
```

Reusable ingestion and loading logic must live under:

```text
src/traffic_data_elt/
```

Do not embed large Python processing logic directly inside DAG files.

Airflow must use PostgreSQL for metadata storage.

Do not install Airflow into the Dev Container.

## PostgreSQL

PostgreSQL is the V1 data warehouse.

Initialize required databases, users, and schemas through version-controlled SQL under:

```text
v1_local/postgres/init/
```

Warehouse schemas should include:

```text
raw
staging
intermediate
marts
analytics
audit
```

Database initialization must be idempotent where practical.

## dbt

Use the existing shared dbt project:

```text
dbt/traffic_dwh/
```

Do not create a separate V1 dbt project.

V1 should use a local PostgreSQL dbt target.

## Redash

Redash should connect to the PostgreSQL warehouse for analytics.

Keep Redash metadata logically separate from warehouse data.

Add Redis only where required by the selected Redash deployment.

## Security

Do not:

- hardcode passwords
- mount the host Docker socket
- commit `.env`
- expose database ports beyond what is required
- add unnecessary privileged containers beyond the existing Docker-in-Docker development environment

## Resource Usage

The local environment must remain suitable for a machine with limited disk and memory.

Prefer:

- minimal service count
- shared PostgreSQL server with logical database separation
- lightweight images
- bounded logs
- no unnecessary package installation

## Implementation Order

1. Define `v1_local/compose.yaml`.
2. Add PostgreSQL initialization.
3. Add Airflow runtime image and configuration.
4. Add Redash and Redis if required.
5. Confirm service dependencies and health checks.
6. Add the first ingestion DAG.
7. Integrate the shared Python package.
8. Add dbt connectivity.
9. Add data quality and audit metadata.

## Fixed Compose Contract

### Services

Use exactly these runtime services unless an architectural change is explicitly approved:

- `postgres`
- `airflow-api-server`
- `airflow-scheduler`
- `airflow-dag-processor`
- `redash-server`
- `redash-worker`
- `redis`

### PostgreSQL Databases

One PostgreSQL server hosts:

- `traffic_dwh`
- `airflow_meta`
- `redash_meta`

Use separate application users for each database.

### Network

Use one Compose network:

`traffic_v1`

### Volumes

Use named volumes:

- `postgres_data`
- `airflow_logs`
- `redash_data`
- `redis_data`

### Exposed Ports

Expose only:

- PostgreSQL: `5432`
- Airflow: `8080`
- Redash: `5000`

Redis must remain internal.

### Configuration

Use `v1_local/.env` for runtime configuration.

Do not hardcode credentials in `compose.yaml`.

### Mounts

Mount only the repository paths required by a service.

Airflow may mount:

- `v1_local/airflow/dags/`
- `src/traffic_data_elt/`

Do not mount the full repository into runtime containers without a clear requirement.

### Health Checks

Add health checks for:

- PostgreSQL
- Airflow API server
- Redis
- Redash server