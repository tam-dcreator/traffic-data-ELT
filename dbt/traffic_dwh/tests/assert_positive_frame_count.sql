/*
  assert_positive_frame_count
  ───────────────────────────
  Every trajectory must have at least one frame.

  A non-zero result count causes this test to fail.
*/

select
    source_file,
    track_id,
    frame_count
from {{ ref('fct_vehicle_trajectories') }}
where frame_count < 1
