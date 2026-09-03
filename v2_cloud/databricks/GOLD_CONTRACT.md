# V2 Gold — Trajectory Summary Contract

This document is the authoritative contract for the first V2 Gold dataset.
It is derived directly from the V1 dbt model
`dbt/traffic_dwh/models/intermediate/int_vehicle_trajectory_summary.sql`
(the source of truth), not from the project context prose.

The Gold transformation moves the logical responsibility of
`intermediate.int_vehicle_trajectory_summary` out of PostgreSQL/dbt and into
Databricks/Spark, while preserving the same semantics.

---

## 1. Grain

**One row per `(source_file, track_id)`.**

`track_id` is **not** globally unique across the full pNEUMA archive — the same
integer identifier can recur in different source CSV files. The composite key
`(source_file, track_id)` is therefore the trajectory identity, matching V1's
`group by source_file, track_id`. The Spark `groupBy` uses both columns.

---

## 2. Grouping key

```
groupBy("source_file", "track_id")
```

---

## 3. Frame-level input preparation (parity-critical)

V1 aggregates over `stg_vehicle_trajectories`, a view that transforms
`raw.vehicle_trajectories` **before** aggregation:

| Staging transform | V1 SQL | Gold Spark equivalent |
|---|---|---|
| vehicle_type normalise | `trim(lower(vehicle_type))` | `trim(lower(...))` — already normalised in Silver by the shared parser, re-applied defensively |
| coordinate rounding | `round(lat::numeric, 6)`, `round(lon::numeric, 6)` | `round(col, 6)` |
| kinematics rounding | `round(speed_ms,4)`, `round(lon_acc_ms2,4)`, `round(lat_acc_ms2,4)` | `round(col, 4)` |
| null filter | `where track_id/vehicle_type/lat/lon/speed_ms/timestamp_s is not null` | `.na.drop(...)` on the same columns |

Silver stores **unrounded** parser floats. To reproduce V1 field-for-field,
Gold applies the same rounding to the frame columns **before** the aggregation.
This makes `min_speed_ms`, `max_speed_ms`, `avg_lon_acc_ms2`, `avg_lat_acc_ms2`,
`max_lon_acc_ms2`, `max_lat_acc_ms2`, and the start/end coordinates match V1
rather than depending only on a loose tolerance.

> `traveled_d_m` and `avg_speed_ms` are track-level constants carried on every
> frame; V1 does **not** round them (they come straight from the raw table via
> `select *`). Gold likewise leaves them unrounded.

Rounding mode note: PostgreSQL `round(numeric, n)` uses round-half-away-from-zero;
Spark `round(col, n)` uses round-half-up (HALF_UP) which is identical for
positive numbers and away-from-zero for the magnitudes here. Residual
representation differences are absorbed by the documented float tolerance in the
parity comparison (see §7).

---

## 4. Output columns — exact V1 → Spark mapping

Grain columns first, then metrics in V1 declaration order.

| # | Gold column | Type | Unit | Meaning | V1 source expression | Spark expression |
|---|---|---|---|---|---|---|
| 1 | `source_file` | string | — | Originating CSV basename | group key | `groupBy` key |
| 2 | `track_id` | int | — | Vehicle id within file | group key | `groupBy` key |
| 3 | `vehicle_type` | string | — | Lowercased vehicle class | `min(vehicle_type)` | `min(vehicle_type)` |
| 4 | `frame_count` | long | count | Number of frame observations | `count(*)` | `count(*)` |
| 5 | `start_time_s` | double | s | Earliest frame timestamp | `min(timestamp_s)` | `min(timestamp_s)` |
| 6 | `end_time_s` | double | s | Latest frame timestamp | `max(timestamp_s)` | `max(timestamp_s)` |
| 7 | `duration_s` | double | s | end − start | `max(ts) - min(ts)` | `max(ts) - min(ts)` |
| 8 | `traveled_d_m` | double | m | Source total distance (const/track) | `min(traveled_d_m)` | `min(traveled_d_m)` |
| 9 | `avg_speed_ms` | double | m/s | Source avg speed (const/track) | `min(avg_speed_ms)` | `min(avg_speed_ms)` |
| 10 | `max_speed_ms` | double | m/s | Max instantaneous speed | `max(speed_ms)` | `max(round(speed_ms,4))` |
| 11 | `min_speed_ms` | double | m/s | Min instantaneous speed | `min(speed_ms)` | `min(round(speed_ms,4))` |
| 12 | `avg_lon_acc_ms2` | double | m/s² | Mean longitudinal acceleration | `avg(lon_acc_ms2)` | `avg(round(lon_acc_ms2,4))` |
| 13 | `avg_lat_acc_ms2` | double | m/s² | Mean lateral acceleration | `avg(lat_acc_ms2)` | `avg(round(lat_acc_ms2,4))` |
| 14 | `max_lon_acc_ms2` | double | m/s² | Max absolute longitudinal acc | `max(abs(lon_acc_ms2))` | `max(abs(round(lon_acc_ms2,4)))` |
| 15 | `max_lat_acc_ms2` | double | m/s² | Max absolute lateral acc | `max(abs(lat_acc_ms2))` | `max(abs(round(lat_acc_ms2,4)))` |
| 16 | `start_lat` | double | deg | Latitude at first frame (by time) | `(array_agg(lat order by ts asc))[1]` | see §5 |
| 17 | `start_lon` | double | deg | Longitude at first frame | `(array_agg(lon order by ts asc))[1]` | see §5 |
| 18 | `end_lat` | double | deg | Latitude at last frame | `(array_agg(lat order by ts desc))[1]` | see §5 |
| 19 | `end_lon` | double | deg | Longitude at last frame | `(array_agg(lon order by ts desc))[1]` | see §5 |

**19 columns total** — identical set and order to V1
`int_vehicle_trajectory_summary` / `fct_vehicle_trajectories`.

Type mapping to Spark: `frame_count` is a `LongType` in Spark (`count(*)`),
which maps cleanly to PostgreSQL `bigint` for the later Neon milestone. V1's
`count(*)` is `bigint`; the fct table carries it as an integer count. No nested
/ complex Spark types are used — the output is fully relational.

---

## 5. Start / end coordinates (first/last by time)

V1 uses `(array_agg(x order by timestamp_s asc))[1]` for the value at the
earliest frame and `... desc` for the latest. The Spark-faithful equivalent
uses a windowed row-number over `(source_file, track_id)` ordered by
`timestamp_s`, then keeps the first row's start/end coordinates — implemented
via `first(...)` over an ordered window, or an equivalent
`Window.partitionBy(key).orderBy(timestamp_s)` with `first_value`/`last_value`
using an unbounded frame. This avoids collecting arrays and is a native Spark
aggregation.

Tie-break: when two frames share the same `timestamp_s` (should not happen — the
parser enforces a strictly increasing timestamp step per track), V1's
`array_agg` order among equal keys is not deterministic. The Gold
implementation adds `lat`/`lon` as a secondary deterministic order only as a
tie-break; for this dataset timestamps are unique per track so the result is
identical to V1.

---

## 6. Invariants (from V1 tests + structural guarantees)

| Invariant | Origin | Gold check |
|---|---|---|
| grain `(source_file, track_id)` unique | `assert_int_trajectory_grain_unique` | duplicate-key count == 0 |
| `frame_count >= 1` | `assert_positive_frame_count` | min(frame_count) >= 1 |
| `traveled_d_m >= 0` | `assert_no_negative_distance` | count(traveled_d_m < 0) == 0 |
| `duration_s >= 0` | `assert_no_negative_duration` | count(duration_s < 0) == 0 |
| all 19 columns not null | `_int_models.yml` not_null on every column | null count per column == 0 |
| `SUM(frame_count) == COUNT(silver rows)` | frame conservation (structural) | equality; sample == 1,446,887 |
| `start_time_s <= end_time_s` | duration definition | count(start > end) == 0 |
| `min_speed_ms >= 0`, `max_speed_ms >= 0` | parser guarantees speed >= 0 | count(< 0) == 0 |
| Gold trajectory count == logical vehicle count | grain | sample == 922 |

Speed and coordinate range tests in V1 run against `raw`, not the summary, so
they are not Gold summary invariants; the Silver validator already enforces
coordinate bounds and non-negative speed at the frame level.

---

## 7. Float tolerance for parity comparison

Integer/count fields (`frame_count`) and categorical fields (`source_file`,
`track_id`, `vehicle_type`) are compared **exactly**.

Floating-point aggregates are compared with an absolute tolerance:

```
FLOAT_TOLERANCE = 1e-6
```

Applied to: `start_time_s`, `end_time_s`, `duration_s`, `traveled_d_m`,
`avg_speed_ms`, `max_speed_ms`, `min_speed_ms`, `avg_lon_acc_ms2`,
`avg_lat_acc_ms2`, `max_lon_acc_ms2`, `max_lat_acc_ms2`, `start_lat`,
`start_lon`, `end_lat`, `end_lon`.

Rationale: Gold replicates V1 rounding, so values should be equal to well
within 1e-6; the tolerance only absorbs IEEE-754 double representation and
`avg()` accumulation-order differences between Spark and PostgreSQL. `1e-6`
is far tighter than the 4-dp / 6-dp rounding granularity of the inputs, so a
genuine semantic error cannot hide beneath it.

---

## 8. Physical output

```
s3://<bucket>/gold/pneuma/trajectory_summary/test/
```

- Format: Parquet, Snappy compression.
- Write mode: `overwrite` (idempotent for this fixture — reruns replace, never
  append).
- No partitioning (922 rows). No `track_id` partitioning. No forced 256 MB
  object target for this sample; small files are expected.

Production implication (deferred): partitioned Gold across many source files
would use `overwrite` with `partitionOverwriteMode=dynamic` or a merge, and
coarse date/area partitions — out of scope for this milestone.

---

## 9. Not in scope

`traffic_metrics`, `vehicle_type_metrics`, time aggregates, and area aggregates
are deferred. Only `trajectory_summary` is built here, proving the
trajectory-summary layer. `dim_vehicle_type` (a downstream V1 mart that reads
the summary) stays a dbt/Neon responsibility and is NOT reimplemented in Gold.
