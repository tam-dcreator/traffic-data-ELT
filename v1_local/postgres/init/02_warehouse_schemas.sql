-- 02_warehouse_schemas.sql
-- Creates the warehouse schemas inside traffic_dwh and grants the application
-- user access to each one.
--
-- Must be connected to traffic_dwh when executed.
-- The postgres container runs each *.sql file in the traffic_dwh database
-- context only if we use the helper script below; otherwise we use \connect.
-- We use explicit \connect so this file is self-contained.
--
-- Idempotent: CREATE SCHEMA IF NOT EXISTS is safe to re-run.

\connect traffic_dwh

-- ── Warehouse schemas ─────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS audit;

-- ── Grant usage and create privileges to the warehouse user ───────────────────
-- These grants cover objects created in future migrations as well as
-- objects that exist now.
DO $$
DECLARE
    schema_name text;
BEGIN
    FOREACH schema_name IN ARRAY ARRAY['raw','staging','intermediate','marts','analytics','audit']
    LOOP
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO traffic_user', schema_name);
        EXECUTE format('GRANT CREATE ON SCHEMA %I TO traffic_user', schema_name);
        -- Default privileges: any table/sequence created by postgres superuser
        -- will also be accessible to traffic_user.
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON TABLES TO traffic_user',
            schema_name
        );
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON SEQUENCES TO traffic_user',
            schema_name
        );
    END LOOP;
END;
$$;
