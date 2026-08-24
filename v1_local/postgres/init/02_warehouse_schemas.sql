-- 02_warehouse_schemas.sql
-- Creates the warehouse schemas inside traffic_dwh and transfers ownership
-- to traffic_user so the application user can manage objects without
-- requiring ongoing superuser grants.
--
-- Execution context: connected to traffic_dwh as the superuser.
-- The postgres container connects each *.sql init file to the default
-- database unless a \connect directive overrides it.
--
-- Idempotent: CREATE SCHEMA IF NOT EXISTS and ALTER SCHEMA OWNER are both
-- safe to re-run against an already-initialised database.

\connect traffic_dwh

-- ── Warehouse schemas ─────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS audit;

-- ── Transfer ownership to the application user ────────────────────────────────
-- Owning the schema grants the application user full DDL rights within it
-- (CREATE TABLE, ALTER TABLE, CREATE INDEX, etc.) without requiring the
-- superuser to issue further grants.  This is correct for a single-tenant
-- local warehouse where traffic_user is the sole application actor.
DO $$
DECLARE
    s text;
BEGIN
    FOREACH s IN ARRAY ARRAY['raw','staging','intermediate','marts','analytics','audit']
    LOOP
        EXECUTE format('ALTER SCHEMA %I OWNER TO traffic_user', s);
    END LOOP;
END;
$$;
