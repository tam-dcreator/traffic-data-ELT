{{
  config(
    materialized = 'view',
    schema = 'intermediate'
  )
}}

/*
  int_vehicle_trajectory_summary
  ──────────────────────────────
  One row per vehicle trajectory (source_file, track_id).

  Aggregates frame-level data from staging into reusable trajectory metrics.
  Downstream marts and analytics models should reference this rather than
  re-deriving trajectory summaries from frame-level rows.

  Notes
  -----
  - traveled_d_m and avg_speed_ms are source-provided values carried at the
    track level (constant across all frames for a given track). We take the
    first value via min() — they are identical within a track.
  - Frame-derived metrics (duration, min/max speed, accelerations) are
    computed from the individual frame observations.
*/

with frames as (

    select
        source_file,
        track_id,
        vehicle_type,
        traveled_d_m,
        avg_speed_ms,
        lat,
        lon,
        speed_ms,
        lon_acc_ms2,
        lat_acc_ms2,
        timestamp_s
    from {{ ref('stg_vehicle_trajectories') }}

),

trajectory_agg as (

    select
        source_file,
        track_id,

        -- Vehicle type is constant per track; take any value.
        min(vehicle_type)                           as vehicle_type,

        -- Frame count.
        count(*)                                    as frame_count,

        -- Time boundaries (from frame timestamps).
        min(timestamp_s)                            as start_time_s,
        max(timestamp_s)                            as end_time_s,
        max(timestamp_s) - min(timestamp_s)         as duration_s,

        -- Source-provided track-level metrics (constant per track).
        min(traveled_d_m)                           as traveled_d_m,
        min(avg_speed_ms)                           as avg_speed_ms,

        -- Frame-derived speed metrics.
        max(speed_ms)                               as max_speed_ms,
        min(speed_ms)                               as min_speed_ms,

        -- Acceleration averages and extremes.
        avg(lon_acc_ms2)                            as avg_lon_acc_ms2,
        avg(lat_acc_ms2)                            as avg_lat_acc_ms2,
        max(abs(lon_acc_ms2))                       as max_lon_acc_ms2,
        max(abs(lat_acc_ms2))                       as max_lat_acc_ms2,

        -- Start/end coordinates (by time).
        (array_agg(lat order by timestamp_s asc))[1]  as start_lat,
        (array_agg(lon order by timestamp_s asc))[1]  as start_lon,
        (array_agg(lat order by timestamp_s desc))[1] as end_lat,
        (array_agg(lon order by timestamp_s desc))[1] as end_lon

    from frames
    group by source_file, track_id

)

select * from trajectory_agg
