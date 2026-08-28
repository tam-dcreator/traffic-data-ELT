-- Query to get the KPIs for the traffic dataset

SELECT
    COUNT(*) AS total_trajectories,
    ROUND(AVG(avg_speed_ms)::numeric, 2) AS avg_speed_ms,
    ROUND(AVG(traveled_d_m)::numeric, 2) AS avg_distance_m,
    ROUND(AVG(duration_s)::numeric, 2) AS avg_duration_s
FROM marts.fct_vehicle_trajectories;
