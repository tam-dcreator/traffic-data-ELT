# Security

## Core Rules

Never hardcode or commit:

- Database passwords
- AWS credentials
- API keys
- Access tokens
- Private keys
- Connection strings containing secrets
- `.env` files
- Local credential directories
- Database dumps
- Raw production data

## Environment Variables

Use environment variables for runtime configuration and secrets.

Commit only template files such as:

```text
.env.example
v1_local/.env.example
```

Never commit:

```text
.env
v1_local/.env
v2_cloud/.env
```

## GitGuardian

GitGuardian is part of the local development workflow.

Generated or modified code must be checked for secrets before commit.

Do not suppress GitGuardian findings without understanding the reason for the detection.

## Docker

Use trusted and pinned base images where practical.

Avoid unnecessary packages and privileges.

Docker-in-Docker is intentionally used for project isolation, but it requires privileged capabilities and must not be treated as a complete security boundary.

Do not expose the host Docker socket unless explicitly approved as an architectural change.

## AWS

Do not place AWS credentials in:

- Source files
- DAGs
- Dockerfiles
- Compose files
- dbt profiles committed to Git

When AWS access is introduced, credentials must use an approved runtime mechanism.

Any host credential mount must be read-only.

## Logging

Do not log secrets or complete connection strings.

Configuration logging should redact sensitive values.

## Dependency Management

Prefer minimal dependencies.

Do not add a package unless it has a clear project requirement.

Pin important runtime dependencies where reproducibility or compatibility requires it.
