#!/usr/bin/env bash
# 01_databases_and_users.sh
# Creates the three logical databases and their application users.
#
# Executed automatically by the postgres container's docker-entrypoint on first
# start.  The superuser connection is already available at this point.
# All credentials are read from environment variables injected by compose.yaml —
# never hardcoded here.
#
# Idempotent: each CREATE DATABASE / CREATE USER is guarded by an existence
# check so re-running on an already-initialised volume is safe.

set -euo pipefail

# Helper: run SQL as the superuser against a given database.
psql_su() {
    local db="$1"
    shift
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" "$@"
}

echo "[init] Creating databases and application users…"

# ── traffic_dwh ───────────────────────────────────────────────────────────────
psql_su postgres <<-EOSQL
    SELECT 'CREATE DATABASE "${TRAFFIC_DB_NAME}"'
    WHERE NOT EXISTS (
        SELECT FROM pg_database WHERE datname = '${TRAFFIC_DB_NAME}'
    )\gexec

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${TRAFFIC_DB_USER}') THEN
            CREATE USER "${TRAFFIC_DB_USER}" WITH PASSWORD '${TRAFFIC_DB_PASSWORD}';
        END IF;
    END;
    \$\$;

    GRANT ALL PRIVILEGES ON DATABASE "${TRAFFIC_DB_NAME}" TO "${TRAFFIC_DB_USER}";
EOSQL

psql_su "$TRAFFIC_DB_NAME" <<-EOSQL
    ALTER DATABASE "${TRAFFIC_DB_NAME}" OWNER TO "${TRAFFIC_DB_USER}";
    ALTER SCHEMA public OWNER TO "${TRAFFIC_DB_USER}";
    GRANT ALL ON SCHEMA public TO "${TRAFFIC_DB_USER}";
EOSQL
echo "[init] Created database ${TRAFFIC_DB_NAME} with user ${TRAFFIC_DB_USER}."

# ── airflow_meta ──────────────────────────────────────────────────────────────
psql_su postgres <<-EOSQL
    SELECT 'CREATE DATABASE "${AIRFLOW_DB_NAME}"'
    WHERE NOT EXISTS (
        SELECT FROM pg_database WHERE datname = '${AIRFLOW_DB_NAME}'
    )\gexec

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${AIRFLOW_DB_USER}') THEN
            CREATE USER "${AIRFLOW_DB_USER}" WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
        END IF;
    END;
    \$\$;

    GRANT ALL PRIVILEGES ON DATABASE "${AIRFLOW_DB_NAME}" TO "${AIRFLOW_DB_USER}";
EOSQL

psql_su "$AIRFLOW_DB_NAME" <<-EOSQL
    ALTER DATABASE "${AIRFLOW_DB_NAME}" OWNER TO "${AIRFLOW_DB_USER}";
    ALTER SCHEMA public OWNER TO "${AIRFLOW_DB_USER}";
    GRANT ALL ON SCHEMA public TO "${AIRFLOW_DB_USER}";
EOSQL
echo "[init] Created database ${AIRFLOW_DB_NAME} with user ${AIRFLOW_DB_USER}."

# ── redash_meta ───────────────────────────────────────────────────────────────
psql_su postgres <<-EOSQL
    SELECT 'CREATE DATABASE "${REDASH_DB_NAME}"'
    WHERE NOT EXISTS (
        SELECT FROM pg_database WHERE datname = '${REDASH_DB_NAME}'
    )\gexec

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${REDASH_DB_USER}') THEN
            CREATE USER "${REDASH_DB_USER}" WITH PASSWORD '${REDASH_DB_PASSWORD}';
        END IF;
    END;
    \$\$;

    GRANT ALL PRIVILEGES ON DATABASE "${REDASH_DB_NAME}" TO "${REDASH_DB_USER}";
EOSQL

psql_su "$REDASH_DB_NAME" <<-EOSQL
    ALTER DATABASE "${REDASH_DB_NAME}" OWNER TO "${REDASH_DB_USER}";
    ALTER SCHEMA public OWNER TO "${REDASH_DB_USER}";
    GRANT ALL ON SCHEMA public TO "${REDASH_DB_USER}";
EOSQL

echo "[init] Databases and users created."
