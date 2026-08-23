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
