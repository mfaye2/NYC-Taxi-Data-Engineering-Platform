SELECT
    MIN(pickup_datetime) AS min_pickup_date,
    MAX(pickup_datetime) AS max_pickup_date,
    COUNT(*) AS trip_count
FROM nyc_taxi_data_platform_dev.curated_trips;