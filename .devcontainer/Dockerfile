FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

RUN rm -f /etc/apt/sources.list.d/yarn.list \
    /usr/share/keyrings/yarn-archive-keyring.gpg

# Repository-level secret scanning and dbt install
RUN pip install --no-cache-dir ggshield==1.53.0 pre-commit==4.3.0 \
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

