{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  fct_vehicle_trajectories
  ────────────────────────
  Fact table: one row per vehicle trajectory (source_file, track_id).

  Built from the intermediate trajectory summary. Exposes a reporting-ready
  fact model for trajectory analysis and dashboard consumption.
*/

with trajectories as (

    select
        source_file,
        track_id,
        vehicle_type,
        frame_count,
        start_time_s,
        end_time_s,
        duration_s,
        traveled_d_m,
        avg_speed_ms,
        max_speed_ms,
        min_speed_ms,
        avg_lon_acc_ms2,
        avg_lat_acc_ms2,
        max_lon_acc_ms2,
        max_lat_acc_ms2,
        start_lat,
        start_lon,
        end_lat,
        end_lon
    from {{ ref('int_vehicle_trajectory_summary') }}

)

select * from trajectories
