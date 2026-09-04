{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  dim_vehicle_type
  ────────────────
  Dimensional summary: one row per normalized vehicle_type.

  Provides aggregate statistics useful for Redash filtering and
  vehicle-type comparisons without scanning the full fact table.
*/

with trajectories as (

    select
        vehicle_type,
        traveled_d_m,
        avg_speed_ms,
        duration_s
    from {{ ref('int_trajectory_summary_source') }}

),

vehicle_type_summary as (

    select
        vehicle_type,
        count(*)                        as trajectory_count,
        avg(traveled_d_m)               as average_traveled_distance_m,
        avg(avg_speed_ms)               as average_speed_ms,
        avg(duration_s)                 as average_duration_s
    from trajectories
    group by vehicle_type

)

select * from vehicle_type_summary
