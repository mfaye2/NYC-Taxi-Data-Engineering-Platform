SELECT
    pickup_zone,
    pickup_borough,
    COUNT(*) AS trip_count,
    ROUND(AVG(total_amount), 2) AS avg_total_amount

FROM {{ ref('stg_trips') }}

GROUP BY
    pickup_zone,
    pickup_borough

ORDER BY trip_count DESC