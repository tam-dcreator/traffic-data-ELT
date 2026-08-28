/*
  assert_int_trajectory_grain_unique
  ───────────────────────────────────
  Validates that the trajectory grain (source_file, track_id) is unique
  in the intermediate trajectory summary.

  A non-zero result count causes this test to fail.
*/

select
    source_file,
    track_id,
    count(*) as row_count
from {{ ref('int_vehicle_trajectory_summary') }}
group by source_file, track_id
having count(*) > 1
