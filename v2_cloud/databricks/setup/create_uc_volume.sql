-- One-time setup: create the Unity Catalog managed volume for V2 temporary
-- processing files.
--
-- Run this SQL once in a Databricks SQL editor or notebook cell before
-- executing the Silver pipeline for the first time.
--
-- The volume holds only temporary working files (downloaded ZIPs and extracted
-- CSVs).  All files are deleted after successful Silver validation.
-- Bronze and Silver on S3 remain the durable data layers.
--
-- Adjust the catalog and schema names to match your workspace.  The defaults
-- below target the built-in `workspace` catalog and `default` schema, which
-- exist in every Databricks trial and standard workspace.

CREATE SCHEMA IF NOT EXISTS workspace.default;

CREATE VOLUME IF NOT EXISTS workspace.default.v2_temp
  COMMENT 'Temporary working volume for V2 Silver pipeline (ZIP extraction, CSV parsing). Contents are deleted after each successful run.';

-- Verify:
SHOW VOLUMES IN workspace.default;
