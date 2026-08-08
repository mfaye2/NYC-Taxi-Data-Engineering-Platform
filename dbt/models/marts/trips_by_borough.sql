SELECT
    pickup_borough,
    COUNT(*) AS trip_count,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance

FROM {{ ref('stg_trips') }}

GROUP BY pickup_borough