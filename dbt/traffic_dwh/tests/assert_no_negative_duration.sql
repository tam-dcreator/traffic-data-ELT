/*
  assert_no_negative_duration
  ───────────────────────────
  Trajectory duration must not be negative.

  A non-zero result count causes this test to fail.
*/

select
    source_file,
    track_id,
    duration_s
from {{ ref('fct_vehicle_trajectories') }}
where duration_s < 0
