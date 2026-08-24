{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

/*
  stg_pipeline_runs
  ─────────────────
  Exposes audit.pipeline_runs for downstream observability models.
  Adds derived columns useful for monitoring dashboards.
*/

select
    run_id,
    dag_id,
    task_id,
    source_file,
    status,
    coalesce(rows_loaded, 0)    as rows_loaded,
    coalesce(rows_rejected, 0)  as rows_rejected,
    error_message,
    started_at,
    finished_at,
    duration_s,

    -- Convenience flags for dashboard filters.
    (status = 'success')        as is_success,
    (status = 'failed')         as is_failed,
    (status = 'skipped')        as is_skipped

from {{ source('audit', 'pipeline_runs') }}
