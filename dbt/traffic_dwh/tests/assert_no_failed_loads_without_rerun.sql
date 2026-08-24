/*
  assert_no_failed_loads_without_rerun
  ─────────────────────────────────────
  Fails if any source file has a 'failed' pipeline_run entry but no
  subsequent 'success' entry.

  A file that failed and was then successfully reloaded should not trigger
  this test.  Only files that are still in a failed state (i.e. have never
  been successfully loaded) are surfaced.

  A non-zero result count causes this test to fail.
*/

with latest_status as (

    select
        source_file,
        bool_or(status = 'success') as ever_succeeded,
        bool_or(status = 'failed')  as ever_failed
    from {{ source('audit', 'pipeline_runs') }}
    group by source_file

)

select source_file
from latest_status
where ever_failed = true
  and ever_succeeded = false
