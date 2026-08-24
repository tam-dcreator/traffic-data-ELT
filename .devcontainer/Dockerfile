FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

RUN rm -f /etc/apt/sources.list.d/yarn.list \
    /usr/share/keyrings/yarn-archive-keyring.gpg

# Repository-level secret scanning.
RUN pip install --no-cache-dir ggshield==1.53.0 pre-commit==4.3.0
