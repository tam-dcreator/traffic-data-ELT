/*
  assert_no_negative_distance
  ───────────────────────────
  Traveled distance must not be negative.

  A non-zero result count causes this test to fail.
*/

select
    source_file,
    track_id,
    traveled_d_m
from {{ ref('fct_vehicle_trajectories') }}
where traveled_d_m < 0
