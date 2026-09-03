FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

RUN rm -f /etc/apt/sources.list.d/yarn.list \
    /usr/share/keyrings/yarn-archive-keyring.gpg

# Repository-level secret scanning and dbt install.
# Upgrade pip first so the tool installs resolve against the latest resolver.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir ggshield==1.53.0 pre-commit==4.3.0 \
    "dbt-core>=1.8,<2" \
    "dbt-postgres>=1.8,<2"

# Databricks CLI (V2 cloud tooling): pinned release, checksum-verified.
# Installed from the official GitHub release rather than a package manager so the
# version and integrity are reproducible. Linux amd64 matches the dev container arch.
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

# Neon CLI (V2 serving warehouse tooling): pinned release, checksum-verified.
# Installed from the official GitHub release rather than npm so the version and
# integrity are reproducible and no Node/npm toolchain is added to the image.
# The linux-x64 asset matches the dev container arch (amd64).
# NOTE: this is the `neon` CLI (agent toolkit: auth, api-keys, projects,
# branches, skills, mcp, deploy) distributed as a standalone binary — not the
# older `neonctl`. The URL uses the concrete tag (not the floating `latest/`
# redirect) so the pinned version and checksum stay reproducible.
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

