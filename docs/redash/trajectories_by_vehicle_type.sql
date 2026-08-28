-- Query to get the number of vehicles per vehicle type

SELECT
    vehicle_type,
    COUNT(*) AS trajectory_count
FROM marts.fct_vehicle_trajectories
GROUP BY vehicle_type
ORDER BY trajectory_count DESC;
