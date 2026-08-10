SELECT
    pickup_year,
    pickup_month,
    COUNT(*) AS trip_count
FROM nyc_taxi_data_platform_dev.curated_trips
GROUP BY
    pickup_year,
    pickup_month
ORDER BY
    pickup_year,
    pickup_month;