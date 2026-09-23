# V2 Airflow runtime image — extends the shared V1 Airflow image with the
# cloud tooling the V2 production DAG needs.
#
# Design
# ------
# The project runs ONE Airflow platform (the V1 stack). V2 does not introduce a
# second, unrelated Airflow deployment; it layers the extra tooling required by
# the V2 production DAG on top of the already-built V1 image:
#
#     apache/airflow:3.0.3
#         └── traffic-airflow:3.0.3        (V1 image: shared package + dbt)
#                 └── traffic-airflow-v2:3.0.3   (this image: + V2 cloud CLIs)
#
# Building FROM the V1 image (rather than editing the V1 Dockerfile) keeps the
# V1 runtime unchanged and keeps the cloud tooling isolated to V2.
#
# What this adds
# --------------
#   * Databricks CLI      — the V2 DAG shells out to `databricks ...`
#                           (jobs submit, volumes, workspace, current-user).
#   * Neon CLI            — control-plane preflight (validate_neon_target.py).
#   * python-dotenv       — AwsConfig/NeonConfig.from_env() + the preflight
#                           script load v2_cloud/.env at runtime.
#
# dbt-core + dbt-postgres are already installed in the V1 base image and are
# reused as-is for the dbt v2_production target.
#
# Reproducibility
# ---------------
# CLI versions + checksums are pinned to EXACTLY the values used by the
# .devcontainer image so the developer environment and the Airflow runtime run
# identical tool versions.
#
# Credentials
# -----------
# NOTHING secret is baked into this image. Databricks (~/.databrickscfg), AWS
# (~/.aws), and the V2 .env are supplied at runtime as read-only mounts /
# environment by the compose overlay. This image only carries tooling.

ARG BASE_IMAGE=traffic-airflow:3.0.3
FROM ${BASE_IMAGE}

# ── System tooling for pinned binary installs (root) ──────────────────────────
USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl unzip ca-certificates; \
    rm -rf /var/lib/apt/lists/*

# ── Databricks CLI — pinned release, checksum-verified ────────────────────────
# Matches .devcontainer/Dockerfile so dev + Airflow run the same CLI version.
ARG DATABRICKS_CLI_VERSION=1.14.1
ARG DATABRICKS_CLI_SHA256=06ecb5a3299a3ef85d0ec7095fa193dd28bc12834f59e9ba399846b3af2fbfe8
RUN set -eux; \
    tmp="$(mktemp -d)"; \
    url="https://github.com/databricks/cli/releases/download/v${DATABRICKS_CLI_VERSION}/databricks_cli_${DATABRICKS_CLI_VERSION}_linux_amd64.zip"; \
    curl -fsSL "$url" -o "$tmp/databricks.zip"; \
    echo "${DATABRICKS_CLI_SHA256}  $tmp/databricks.zip" | sha256sum -c -; \
    unzip -q "$tmp/databricks.zip" -d "$tmp"; \
    install -m 0755 "$tmp/databricks" /usr/local/bin/databricks; \
    rm -rf "$tmp"; \
    databricks --version

# ── Neon CLI — pinned release, checksum-verified ──────────────────────────────
# Matches .devcontainer/Dockerfile. This is the standalone `neon` binary
# (control-plane: auth, api, projects, branches), not the older `neonctl`.
ARG NEON_VERSION=4.14.0
ARG NEON_SHA256=3ed6a3690e82fbb8e77d214b269fcd181a2e4f336cd12dae6a748473a5b5b61a
RUN set -eux; \
    tmp="$(mktemp -d)"; \
    url="https://github.com/neondatabase/neon-pkgs/releases/download/neon@${NEON_VERSION}/neon-linux-x64"; \
    curl -fsSL "$url" -o "$tmp/neon"; \
    echo "${NEON_SHA256}  $tmp/neon" | sha256sum -c -; \
    install -m 0755 "$tmp/neon" /usr/local/bin/neon; \
    rm -rf "$tmp"; \
    neon --version

# ── Runtime Python dependency for V2 config/.env loading (airflow user) ───────
# python-dotenv is used by AwsConfig/NeonConfig.from_env() and
# scripts/validate_neon_target.py to read v2_cloud/.env at runtime.
USER airflow
RUN pip install --no-cache-dir "python-dotenv>=1.0,<2"
