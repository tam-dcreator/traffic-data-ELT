{{
  config(
    materialized = ('view' if is_v2_target() else 'table'),
    schema = 'marts'
  )
}}

/*
  fct_vehicle_trajectories
  ────────────────────────
  Fact: one row per vehicle trajectory (source_file, track_id).

  Built from the target-aware semantic adapter (int_trajectory_summary_source,
  ephemeral), so the exact same model serves both environments:

    V1        → adapter inlines int_vehicle_trajectory_summary  (materialized as a TABLE)
    V2 (v2_*) → adapter inlines Neon serving.gold_trajectory_summary (materialized as a VIEW)

  Materialization is target-aware.  Under V2 the serving table already holds
  physical trajectory-level rows, so re-materialising a full copy would only
  duplicate storage in the deliberately constrained Neon budget — a VIEW is
  used instead.  Under V1 the historical TABLE materialization is preserved.
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
    from {{ ref('int_trajectory_summary_source') }}

)

select * from trajectories
