/*
  assert_trajectory_grain_unique
  ──────────────────────────────
  Validates that the trajectory grain (source_file, track_id) is unique
  in both the intermediate summary and the fact table.

  A non-zero result count causes this test to fail.
*/

select
    source_file,
    track_id,
    count(*) as row_count
from {{ ref('fct_vehicle_trajectories') }}
group by source_file, track_id
having count(*) > 1
