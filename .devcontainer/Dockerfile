FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

RUN rm -f /etc/apt/sources.list.d/yarn.list \
    /usr/share/keyrings/yarn-archive-keyring.gpg

# Repository-level secret scanning and dbt install
RUN pip install --no-cache-dir ggshield==1.53.0 pre-commit==4.3.0 \
    "dbt-core>=1.8,<2" \
    "dbt-postgres>=1.8,<2"

