# Traffic Data ELT

A Data Engineering portfolio project that demonstrates how an ELT platform can evolve from a local prototype into a cloud-scale architecture while preserving reusable transformation and business logic.

The project uses the pNEUMA traffic dataset and focuses on orchestration, warehousing, transformation, data quality, observability, reproducibility, and architectural scaling.

## Architecture Overview

The repository contains two implementations of the same platform.

### V1 — Local Prototype

V1 is designed to run locally with Docker.

Planned stack:

- Apache Airflow — orchestration
- PostgreSQL — data warehouse
- dbt Core — transformations, testing, and documentation
- Redash — analytics and dashboards
- Python — reusable ingestion and utility logic

High-level flow:

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

### V2 — Cloud Scale

V2 demonstrates how the same project can scale to larger data volumes using cloud storage and distributed processing.

Planned stack:

- Local Apache Airflow — orchestration
- AWS S3 — object storage
- Databricks / Spark — large-scale processing
- Cloud PostgreSQL — serving warehouse
- dbt Core — shared transformation project

High-level flow:

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

## Engineering Principles

- Keep V1 and V2 in a single monorepo.
- Reuse transformation and business logic wherever practical.
- Keep Airflow DAGs thin and orchestration-focused.
- Place reusable Python logic under `src/traffic_data_elt/`.
- Maintain one shared dbt project for both environments.
- Separate runtime infrastructure from the development environment.
- Keep secrets out of source control.
- Prefer lightweight and reproducible containers.
- Add dependencies only when required.

## Repository Structure

```text
.
├── .devcontainer/
│   ├── Dockerfile
│   ├── SETUP.md
│   └── devcontainer.json
├── .github/
│   └── workflows/
│       └── security.yml
├── .kiro/
│   └── steering/
│       ├── architecture.md
│       ├── data-engineering.md
│       ├── git-workflow.md
│       ├── project.md
│       └── security.md
├── data/
│   └── sample/
├── dbt/
│   └── traffic_dwh/
│       ├── analyses/
│       ├── macros/
│       ├── models/
│       │   ├── intermediate/
│       │   ├── marts/
│       │   └── staging/
│       ├── seeds/
│       ├── snapshots/
│       └── tests/
├── docs/
│   ├── architecture/
│   ├── decisions/
│   └── runbooks/
├── scripts/
├── src/
│   └── traffic_data_elt/
│       ├── config/
│       ├── extract/
│       ├── load/
│       ├── transform/
│       └── utils/
├── tests/
│   ├── integration/
│   └── unit/
├── v1_local/
│   ├── airflow/
│   ├── postgres/
│   └── redash/
└── v2_cloud/
    ├── airflow/
    ├── aws/
    ├── databricks/
    └── postgres/
```

## Warehouse Layers

The warehouse is organized into logical data layers:

```text
raw
staging
intermediate
marts
analytics
audit
```

- `raw` — minimally transformed source data
- `staging` — cleaned, renamed, typed, and standardized records
- `intermediate` — reusable transformation logic
- `marts` — business-oriented analytical models
- `analytics` — reporting-ready datasets
- `audit` — pipeline and data quality metadata

## Development Environment

The development environment is intentionally isolated:

```text
Windows
  -> WSL2
      -> Kiro
          -> Dev Container
              -> Docker-in-Docker
                  -> project runtime containers
```

Docker-in-Docker is intentional for this project and avoids exposing the host Docker socket directly to the development container.

### Core Development Tools

- Python 3.12
- Git
- Docker
- Docker Compose
- Dev Containers
- Kiro
- GitGuardian `ggshield`
- pre-commit

## Python Setup

Project dependencies are defined in `pyproject.toml`.

Install the project and development dependencies with:

```bash
pip install -e ".[dev]"
```

Current core dependencies include:

- pandas
- psycopg


## Environment Configuration

The repository uses committed template files and ignored local environment files.

Root configuration:

```text
.env.example   # committed template
.env           # local only
```

V1 configuration:

```text
v1_local/.env.example   # committed template
v1_local/.env           # local only
```

Create the root local environment file with:

```bash
cp .env.example .env
```

Do not commit real `.env` files.

## Secret Scanning

The repository uses GitGuardian at multiple layers.

### Local CLI

`ggshield` is installed in the Dev Container and is used for repository-level secret scanning.

Run ```ggshield auth login ``` in your terminal to authenticate

### Pre-commit

The repository contains:

```text
.pre-commit-config.yaml
```

Install the Git hook with:

```bash
pre-commit install
```

This runs GitGuardian scanning against staged changes before a commit is created.

### GitHub Actions

The repository contains:

```text
.github/workflows/security.yml
```

The workflow requires the following GitHub Actions repository secret:

```text
GITGUARDIAN_API_KEY
```

Recommended GitGuardian token scope:

```text
scanning
└── scan
```

Use the minimum required scope and a finite token expiration

## Data Quality

Data quality checks will cover relevant cases such as:

- null constraints
- uniqueness
- accepted values
- referential integrity
- malformed records
- invalid timestamps
- row-count anomalies
- impossible coordinates or measurements

Critical failures should stop downstream processing.

