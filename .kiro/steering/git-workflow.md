# Git Workflow

## Branching

Use short-lived feature branches.

Recommended naming:

```text
feature/<name>
fix/<name>
chore/<name>
docs/<name>
```

Examples:

```text
feature/v1-airflow-ingestion
feature/dbt-staging-models
fix/postgres-init
docs/architecture-decision
```

## Commits

Keep commits small and focused.

Use Conventional Commit style where practical:

```text
feat:
fix:
chore:
docs:
refactor:
test:
ci:
```

Examples:

```text
feat: add v1 airflow ingestion pipeline
chore: add project dependency configuration
docs: document local architecture
fix: prevent duplicate raw data loads
```

## Pull Requests

Each pull request should:

- Address one logical change
- Have a clear title
- Explain architectural or behavioral changes
- Include relevant tests
- Avoid unrelated formatting or refactoring

## Secrets

Never commit secrets.

Before committing:

- Review staged files
- Resolve GitGuardian findings
- Ensure `.env` files are not staged
- Ensure credentials and private keys are not included

## Generated Files

Do not commit generated runtime artifacts unless explicitly required.

Examples include:

```text
__pycache__/
.pytest_cache/
dbt/traffic_dwh/target/
dbt/traffic_dwh/logs/
Airflow logs
local data extracts
```

## Testing

Run relevant tests before merging.

Changes to shared Python logic should include unit tests where appropriate.

Changes to data transformations should include dbt tests where appropriate.

## Main Branch

Keep the main branch in a reproducible and runnable state.

Do not commit experimental or partially broken infrastructure directly to main.
