/*
  assert_no_failed_loads_without_rerun
  ─────────────────────────────────────
  Fails if any source file has a 'failed' pipeline_run in the last 24 hours
  and has never had a successful load.

  Rationale
  ---------
  A file that failed and was then successfully reloaded is healthy — this
  test must not surface it.  A file that has only ever failed (and attempted
  a run recently) indicates a pipeline that needs attention.

  The 24-hour window avoids false alarms on historical failures that occurred
  before the pipeline was fixed.  For local development this is a reasonable
  operational horizon; adjust the interval for tighter SLAs.

  A non-zero result count causes this test to fail.
*/

with recent_failures as (

    select distinct source_file
    from {{ source('audit', 'pipeline_runs') }}
    where status     = 'failed'
      and started_at >= now() - interval '24 hours'

),

ever_succeeded as (

    select distinct source_file
    from {{ source('audit', 'pipeline_runs') }}
    where status = 'success'

)

select rf.source_file
from recent_failures rf
left join ever_succeeded es using (source_file)
where es.source_file is null
