/*
  assert_no_negative_speed
  ─────────────────────────
  Fails if any row in raw.vehicle_trajectories has a negative instantaneous
  speed.  Speed is a scalar magnitude and must be >= 0.

  A non-zero result count causes this test to fail.
*/

select
    id,
    source_file,
    track_id,
    timestamp_s,
    speed_ms
from {{ source('raw', 'vehicle_trajectories') }}
where speed_ms < 0
