# Dev Container Setup

This document records the development environment setup for the `traffic_data_ELT` project.

## Environment Overview

The project development environment uses:

- Dev Container CLI
- Docker
- Docker-in-Docker


## Why this Setup

One of the goals of this project was to build a data ingestion and transformation pipeline with autonomous ai agents as assistants. A devcontainer approach was used to create a default scope limit for the agents. The execution of this project required spinning up other docker instances. Docker-in-docker setup was used to provide a separate Docker daemon for the development environment, this enabled the execution of the project while limiting access of the autonomous AI agents to the project container.


## Dev Container CLI

The Dev Container CLI was installed globally with npm as the ide did not expose the standard VS Code devcontainer command:

```text
npm install -g @devcontainers/cli
```

Verify with:

```bash
devcontainer --version
```

## Dev Container Configuration

The Dev Container is defined by:

```text
.devcontainer/devcontainer.json
```

The container uses the Microsoft Python 3.12 Bookworm Dev Container image.

The container user is:

```text
vscode
```

`vscode` refers to the Linux user created inside the Dev Container image. 
It does not indicate that VS Code is being used as the IDE.

## Docker-in-Docker

Docker-in-Docker is intentionally enabled for this project.

The project involves creating and managing Docker containers, so the development environment needs Docker capabilities without directly exposing the host Docker daemon through `/var/run/docker.sock`.



## Yarn Repository Build Issue

The original Dev Container build failed while installing the Docker-in-Docker feature.

The failure was caused by an invalid or missing GPG key for the Yarn APT repository

A custom Dockerfile was therefore created:

```text
.devcontainer/Dockerfile
```

It starts from the official Python Dev Container image and removes the stale Yarn repository before the Docker-in-Docker feature is installed.

This preserves normal APT GPG verification and does not disable package signature checking.

## CLI Tooling

The Dev Container provides the following CLIs:

- AWS CLI and Terraform — installed via Dev Container features in `devcontainer.json`.
- Databricks CLI — installed in the `Dockerfile` from the official pinned GitHub
  release (`linux/amd64`), verified against its published SHA-256 checksum.
- Neon CLI (`neon`) — installed in the `Dockerfile` from the official pinned
  GitHub release (`neon-linux-x64`), verified against its published SHA-256
  checksum.

The Databricks CLI is required for V2 cloud work (OAuth login, `databricks fs`
uploads, and Asset Bundle deployment). There is no first-party Dev Container
feature for it, so it is pinned in the `Dockerfile` to keep the version and
integrity reproducible. The version is controlled by the `DATABRICKS_CLI_VERSION`
build arg.

To bump the version, update both `DATABRICKS_CLI_VERSION` and
`DATABRICKS_CLI_SHA256` (the `linux_amd64.zip` checksum from the
[release page](https://github.com/databricks/cli/releases)), then rebuild.

Verify inside the container with:

```bash
databricks --version
```

### Neon CLI (`neon`)

The Neon CLI is required for the V2 serving-warehouse work (authenticating to
Neon, managing projects/branches, and retrieving connection strings for the
shared dbt project and psycopg loaders).

The `neon` CLI (the agent toolkit: `neon auth`, `api-keys`, `projects`,
`branches`, `skills`, `mcp`, `deploy`) is installed the same way as the
Databricks CLI: the official standalone `neon-linux-x64` binary is downloaded
from the pinned GitHub release and verified against its SHA-256 checksum before
being placed on `PATH`. This deliberately avoids a global npm install
(`npm i -g neon@latest`) — no other tool in the image needs Node/npm, so
keeping it out avoids an unnecessary toolchain and image bloat. The version is
controlled by the `NEON_VERSION` build arg.

> The `neon` binary is pinned via its **concrete release tag**
> (`neon@<version>`), not the floating `.../releases/latest/download/...`
> redirect, so the version and checksum stay reproducible across rebuilds.
> This is the `neon` CLI, not the older `neonctl`.

#### Authentication in a headless container

Interactive OAuth (`neon auth`) opens a browser via `xdg-open`, which **fails in
this headless dev container** — there is no browser and the localhost OAuth
callback is not reachable. This is an environment limitation, not a CLI defect;
it affects any browser-OAuth CLI (both `neon` and `neonctl`).

Authenticate non-interactively with a Neon **API key** instead. The key lives in
the untracked `v2_cloud/.env` file (never in the committed `v2_cloud/.env.example`
and never elsewhere in the repository):

```bash
# 1. Create the key once from a machine with a browser
#    (Neon console → Account settings → API keys).
# 2. Put it in v2_cloud/.env:
#       NEON_API_KEY=<your key>
# 3. Load v2_cloud/.env into the shell, then verify without a browser:
set -a; source v2_cloud/.env; set +a
#4. Test that it was loaded successfully
test -n "$NEON_API_KEY" && echo "NEON_API_KEY is set"
neon me
```

The `neon` CLI reads `NEON_API_KEY` automatically (it is the default for the
`--api-key` flag), so no `neon auth` browser step is needed once the key is set.

`v2_cloud/.env` is gitignored; only `v2_cloud/.env.example` (placeholders only)
is committed. The Neon PostgreSQL connection parameters used later by the shared
dbt project / psycopg loaders (`NEON_DB_HOST`, `NEON_DB_PORT`, `NEON_DB_NAME`,
`NEON_DB_USER`, `NEON_DB_PASSWORD`, `NEON_DB_SSLMODE`) live in the same file —
retrieve them from the Neon console or `neon connection-string <branch>`.

To bump the version, update both `NEON_VERSION` and `NEON_SHA256` (the
`neon-linux-x64` checksum from the concrete tag on the
[release page](https://github.com/neondatabase/neon-pkgs/releases)), then
rebuild.

Verify inside the container with:

```bash
neon --version
```

## Container Build

The Dev Container was successfully built with:

```bash
devcontainer build --workspace-folder .
```

The Docker-in-Docker feature completed successfully after the Yarn repository workaround was added.

## Starting the Container

The container can be started with:

```bash
devcontainer up --workspace-folder .
```

A successful startup reports:

```text
"outcome":"success"
```

## Entering the Container

To open a shell inside the running Dev Container:

```bash
devcontainer exec --workspace-folder . bash
```

Expected environment:

```text
User: vscode
Workspace: /workspaces/traffic_data_ELT
Python: 3.12.x
```

## Rebuild

If the Dockerfile or Dev Container configuration changes, you can rebuild with:

```bash
devcontainer build --workspace-folder .
```

Then:

```bash
devcontainer up --workspace-folder .
```
