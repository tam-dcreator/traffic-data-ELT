{{
  config(
    materialized = 'ephemeral',
    tags = ['serving', 'v2'],
  )
}}

/*
  int_trajectory_summary_source
  ─────────────────────────────
  Target-aware semantic adapter for the trajectory-summary grain.

  This is the single shared seam between the two upstreams that produce the
  identical 19-column trajectory grain (one row per source_file, track_id):

    V1        → int_vehicle_trajectory_summary   (dbt, computed from staging in PG)
    V2 (v2_*) → serving.gold_trajectory_summary  (Spark Gold, loaded into Neon)

  Downstream marts (fct_vehicle_trajectories, dim_vehicle_type) reference THIS
  model instead of either concrete upstream, so the marts stay shared and their
  business semantics are defined exactly once.

  Selection by target
  -------------------
  - is_v2_target() (target name starts with 'v2_')  → read the Neon serving
    source.  The V1 staging/intermediate chain is NOT built or referenced.
  - otherwise (e.g. v1_local) → read the V1 intermediate model, preserving V1
    behaviour exactly (pure passthrough).

  Materialized EPHEMERAL: it creates no physical relation.  It is inlined as a
  CTE into the consuming marts, so there is no redundant Neon view over the
  serving table — the marts read the serving source (V2) or the V1 intermediate
  model directly, while the semantic seam remains a single reusable definition.
*/

{% if is_v2_target() %}

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
from {{ source('v2_serving', 'gold_trajectory_summary') }}

{% else %}

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

{% endif %}
