# V2 Airflow runtime — deployment & runbook

The V2 production DAG (`traffic_data_v2_production`) runs on the project's **one
shared Airflow runtime** — the V1 local stack — extended by a Docker Compose
overlay and an image layer. There is intentionally **no second Airflow
platform**.

This document is the map for the wiring: how to bring it up, and — importantly —
**which parts are portable and which exist only because of the local-Docker
deployment topology.** If you move this DAG to a managed orchestrator (MWAA,
Databricks Workflows, Astronomer, self-managed Kubernetes), read the
[Portability](#portability-what-travels-and-what-does-not) section first: several
of the fixes below are patches for *this* environment and have different (or no)
equivalents elsewhere.

---

## Files

| File | Purpose |
|------|---------|
| `Dockerfile` | `traffic-airflow-v2:3.0.3`, built **FROM** the V1 image `traffic-airflow:3.0.3`; adds the Databricks CLI, Neon CLI, and `python-dotenv`. No secrets baked in. |
| `compose.v2.yaml` | Compose **overlay** on top of `v1_local/compose.yaml`; adds V2 env, mounts, and the V2 image to the four Airflow services. |
| `stage_credentials.sh` | Stages the host's Databricks + AWS credentials into a git-ignored dir the containers can read. Run once before bring-up. |
| `.gitignore` | Ignores `.runtime-credentials/` (staged secrets — never committed). |
| `dags/traffic_data_v2_production.py` | The V2 production DAG (orchestration only). |

---

## Bring-up (local Docker / dev container)

Prerequisites: authenticated Databricks CLI (`databricks auth login`), AWS
credentials configured (`~/.aws`), and a populated **git-ignored** `v2_cloud/.env`
(copy from `v2_cloud/.env.example`).

```bash
# 1. Stage host credentials for the containers (copies, originals untouched).
bash v2_cloud/airflow/stage_credentials.sh

# 2. Build the V1 base image, then the V2 image on top.
cd v1_local
docker compose -f compose.yaml build airflow-api-server
docker compose -f compose.yaml -f ../v2_cloud/airflow/compose.v2.yaml build

# 3. Initialise Airflow metadata (run once), then start the stack.
docker compose -f compose.yaml -f ../v2_cloud/airflow/compose.v2.yaml up airflow-init
docker compose -f compose.yaml -f ../v2_cloud/airflow/compose.v2.yaml up -d
```

Both `ingest_pneuma_raw` (V1) and `traffic_data_v2_production` (V2) will be
discoverable in the single DAGs folder.

Re-run `stage_credentials.sh` whenever you refresh `databricks auth login` or
rotate AWS credentials.

### Credential scope (LocalExecutor)

The stack uses `LocalExecutor`, so task processes run **inside the
`airflow-scheduler` container only**. Cloud credentials (Databricks profile +
OAuth token cache, AWS config/credentials) are therefore mounted on
`airflow-scheduler` **alone** — not on `airflow-api-server`, `airflow-dag-processor`,
or `airflow-init`. Those services do not execute tasks, and DAG parsing reads
only non-secret env, so they need no credential material. This keeps the
credential blast radius to the single service that requires it. Nothing secret
is baked into any image. (If the executor ever changes — e.g. to
Celery/Kubernetes — the credentials must follow wherever tasks actually run:
the workers.)

---

## Pre-production gates

Before the first full-archive trigger, confirm every gate is green (see the
session checkpoint report). In particular the DAG's first task,
`preflight_config`, now **fails fast** if the runtime layout is wrong (missing
`REPO_ROOT`/`pyproject.toml`, `DBT_PROJECT_DIR`/`dbt_project.yml`, or any of the
notebook sources under `NOTEBOOKS_DIR`), with a message naming the env var to
fix. This is what makes a misconfigured *other* environment fail early and
legibly instead of deep inside the wheel or notebook step.

The production write is gated behind `NEON_BRANCH=production` **and**
`ALLOW_PRODUCTION_WRITE=true`. The latter is deliberately left unset until you
intend the real full-archive run.

Data-size clarification: the ~15 GB figure is the **Bronze source input** — the
pNEUMA archive streamed from `ZENODO_URL` into S3 Bronze, then parsed/aggregated
by Databricks/Spark. It is **not** written into Neon. Neon receives only the
compact **19-column Gold trajectory-summary serving dataset** (one row per
vehicle trajectory), which is orders of magnitude smaller. The heavy
frame-level data stays in S3 (Silver/Gold Parquet); Neon is the compact serving
warehouse only.

---

## Environment fixes: what/why/portable?

The setup required several fixes. Each is codified, but most are patches for the
**local-Docker deployment topology** (repo bind-mounted from a host owned by
uid 1000; Airflow containers run as uid 50000; image layers are read-only). The
table states plainly whether each is portable.

| Fix | Where codified | Why it's needed | Portable? |
|-----|----------------|-----------------|-----------|
| **Wheel build from read-only source** (`_stage_build_source` / `build_wheel(output_dir=...)`) | `src/traffic_data_elt/databricks/bootstrap.py` (shared package) | setuptools writes an in-tree `src/*.egg-info` during `pip wheel`, which fails when the repo is a read-only mount. The helper stages the minimal build inputs to a writable dir first. | **Yes — portable.** Lives in shared code; helps in any environment with a non-writable source tree. The one fix that genuinely belongs in the package. |
| **git `safe.directory`** | `compose.v2.yaml` env (`GIT_CONFIG_COUNT`/`KEY_0`/`VALUE_0`) | The repo mount is owned by the host uid (1000); the container runs as uid 50000, so git's "dubious ownership" guard blocks `rev-parse`, and the wheel loses its SHA stamp. | **Local-only.** It's a symptom of the uid-mismatched bind-mounted git checkout. Managed orchestrators don't build the wheel from a git working tree (see below), so this knob is usually irrelevant there. |
| **Credential staging** (`stage_credentials.sh` → group-0-readable copies, mounted read-only; Databricks token cache mounted read-write) | `stage_credentials.sh` + `compose.v2.yaml` volumes | Host dotfiles are `0600` owned by uid 1000; the uid-50000 container can't read them. We stage group-0-readable copies. The Databricks OAuth token cache (`~/.databricks/token-cache.json`) must be writable so the CLI can refresh tokens. | **Local-only pattern.** The *need* (supply credentials) is universal; this *mechanism* is not. Other platforms use their native secret mechanism (see mapping below). |
| **DAGs writable-volume trick** (`airflow_dags_root` volume as the dags parent; V1 and V2 mounted as `/dags/v1`, `/dags/v2`) | `compose.v2.yaml` | You cannot create a nested bind-mount inside a read-only image layer. Backing the dags folder with a writable named volume lets both DAG dirs mount as subdirs, discovered by Airflow's recursive scan. | **Local-only.** A Docker bind-mount concern. Other orchestrators supply DAGs differently (git-sync, S3 sync, baked image). |
| **Read-only repo mount at `REPO_ROOT`** | `compose.v2.yaml` volumes + DAG `REPO_ROOT` default | Gives the wheel build + preflight scripts the whole repo without a writable copy. | **Local-only default.** The DAG reads `REPO_ROOT` from env; other environments override it (or don't need a repo at all). |
| **`python-dotenv` in the image** | `Dockerfile` | `AwsConfig`/`NeonConfig.from_env()` and `validate_neon_target.py` load `v2_cloud/.env`. | **Portable enough** — it's just a dependency. Managed orchestrators typically inject env directly and may not use a `.env` file at all. |

---

## Portability: what travels and what does not

The DAG body is portable — it's orchestration only, delegating to the shared
`traffic_data_elt` package. What is **not** portable is this directory
(`v2_cloud/airflow/`): the overlay, credential staging, and image layer are all
specific to the local-Docker deployment.

The DAG's runtime assumptions are **env-driven** and must be overridden per
platform. Defaults (local Docker) shown; override all that don't match:

| Env var | Local default | What it must point at |
|---------|---------------|-----------------------|
| `REPO_ROOT` | `/opt/airflow/repo` | Repo root containing `pyproject.toml` + `src/` (only needed when a wheel must be *built*; pre-deploy the wheel to skip). |
| `DBT_PROJECT_DIR` | `/opt/airflow/dbt/traffic_dwh` | The shared dbt project. |
| `NOTEBOOKS_DIR` | `/opt/airflow/repo/v2_cloud/databricks/notebooks` | The V2 notebook sources. |
| `V2_ENV_FILE` | `/opt/airflow/repo/v2_cloud/.env` | Non-secret + secret V2 config (or inject the vars directly). |
| `DATABRICKS_PROFILE`, `UC_*`, `ARTIFACT_PATH`, `NEON_*` | see DAG | Databricks/UC/Neon targeting. |

`preflight_config` validates the first three exist and fails fast otherwise.

### Equivalents on managed orchestrators

The recurring questions on any platform are the same three: **how is the wheel
built/deployed, how are credentials supplied, and where do the notebooks come
from.** The local-Docker answers above map as follows.

- **Amazon MWAA**
  - *Wheel:* don't build in-DAG — pre-build in CI and deploy the SHA-stamped
    wheel to the UC artifact volume (`scripts/deploy_databricks_artifact.py`);
    `ensure_versioned_wheel` then reuses it. No git checkout ⇒ `safe.directory`
    not needed.
  - *Credentials:* AWS via the MWAA **execution role** (no `~/.aws`, no staging).
    Databricks/Neon via **Airflow connections** / Secrets Manager backend, not a
    mounted token cache.
  - *DAGs:* synced from S3; no writable-volume trick.
- **Databricks Workflows / Asset Bundles**
  - *Wheel + notebooks:* deployed as bundle artifacts; `ensure_databricks_notebooks`
    and the artifact upload are largely subsumed by the bundle.
  - *Credentials:* native workspace auth + **secret scopes**; no CLI token cache
    to mount.
- **Self-managed Kubernetes (KubernetesExecutor) / Astronomer**
  - *Wheel:* bake into the task image in CI (skip the in-DAG build), or mount a
    writable workspace; the `build_wheel` staging helper covers a read-only source.
  - *Credentials:* K8s `Secret`s / IRSA / a secrets-backend, not `stage_credentials.sh`.
  - *DAGs:* git-sync sidecar or baked image; no bind-mount nesting problem.

In all three, the **portable core** is unchanged: the DAG plus the shared
`traffic_data_elt` package (including the read-only-source wheel-build staging).
Only this `v2_cloud/airflow/` glue layer is replaced by the platform's native
mechanisms.
