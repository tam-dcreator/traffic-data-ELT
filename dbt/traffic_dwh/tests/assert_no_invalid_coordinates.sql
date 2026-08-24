/*
  assert_no_invalid_coordinates
  ──────────────────────────────
  Fails if any row in raw.vehicle_trajectories has coordinates outside the
  Athens metropolitan bounding box used in the pNEUMA experiment.

  Expected bounds (generous margin around the 1.3 km² study area):
    lat : 37.90 – 38.10
    lon : 23.60 – 23.90

  A non-zero result count causes this test to fail.
*/

select
    id,
    source_file,
    track_id,
    lat,
    lon
from {{ source('raw', 'vehicle_trajectories') }}
where
    lat < 37.90
    or lat > 38.10
    or lon < 23.60
    or lon > 23.90
