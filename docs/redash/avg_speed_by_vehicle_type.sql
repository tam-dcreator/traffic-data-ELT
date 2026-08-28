-- Query for the average speed by vehicle

SELECT
    vehicle_type,
    ROUND(AVG(avg_speed_ms)::numeric, 2) AS avg_speed_ms
FROM marts.fct_vehicle_trajectories
GROUP BY vehicle_type
ORDER BY avg_speed_ms DESC;
