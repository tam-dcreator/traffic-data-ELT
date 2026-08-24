{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

/*
  stg_vehicle_trajectories
  ────────────────────────
  Cleans, casts, and renames raw.vehicle_trajectories.

  Changes from raw
  ----------------
  - Columns renamed to snake_case with explicit units in the name.
  - vehicle_type trimmed and lowercased for consistent downstream joins.
  - Coordinates rounded to 6 decimal places (sub-metre precision sufficient).
  - Speeds and accelerations rounded to 4 decimal places.
  - timestamp_s preserved as-is (float seconds from recording start).
  - ingested_at carried through for lineage.
  - id and source_file carried through for traceability.

  Rows excluded
  -------------
  - Records with NULL in any mandatory column are excluded and logged
    via a separate dbt test (see _sources.yml not_null tests).
*/

with source as (

    select * from {{ source('raw', 'vehicle_trajectories') }}

),

cleaned as (

    select
        id                                              as raw_id,
        source_file,
        track_id,
        trim(lower(vehicle_type))                       as vehicle_type,
        traveled_d_m,
        avg_speed_ms,

        -- Coordinates rounded to 6 d.p. (~0.1 m precision).
        round(lat::numeric, 6)::double precision        as lat,
        round(lon::numeric, 6)::double precision        as lon,

        -- Kinematics rounded for readability; retain enough precision
        -- for any downstream statistical work.
        round(speed_ms::numeric, 4)::double precision   as speed_ms,
        round(lon_acc_ms2::numeric, 4)::double precision as lon_acc_ms2,
        round(lat_acc_ms2::numeric, 4)::double precision as lat_acc_ms2,

        timestamp_s,
        ingested_at

    from source

    where
        track_id      is not null
        and vehicle_type is not null
        and lat           is not null
        and lon           is not null
        and speed_ms      is not null
        and timestamp_s   is not null

)

select * from cleaned
