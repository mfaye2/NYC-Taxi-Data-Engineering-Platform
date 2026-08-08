-- 1. Nombre total de trajets valides
SELECT COUNT(*) AS total_trips
FROM nyc_taxi_data_platform_dev.curated_trips;


-- 2. Nombre de trajets par borough de départ
SELECT
    pickup_borough,
    COUNT(*) AS trip_count
FROM nyc_taxi_data_platform_dev.curated_trips
GROUP BY pickup_borough
ORDER BY trip_count DESC;


-- 3. Nombre de trajets par heure
SELECT
    pickup_hour,
    COUNT(*) AS trip_count
FROM nyc_taxi_data_platform_dev.curated_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;


-- 4. Montant moyen par borough
SELECT
    pickup_borough,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM nyc_taxi_data_platform_dev.curated_trips
GROUP BY pickup_borough
ORDER BY avg_total_amount DESC;


-- 5. Distance moyenne par borough
SELECT
    pickup_borough,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance
FROM nyc_taxi_data_platform_dev.curated_trips
GROUP BY pickup_borough
ORDER BY avg_trip_distance DESC;


-- 6. Trajets pendant les heures de pointe
SELECT
    is_peak_hour,
    COUNT(*) AS trip_count
FROM nyc_taxi_data_platform_dev.curated_trips
GROUP BY is_peak_hour;


-- 7. Top 10 zones de départ
SELECT
    pickup_zone,
    COUNT(*) AS trip_count
FROM nyc_taxi_data_platform_dev.curated_trips
GROUP BY pickup_zone
ORDER BY trip_count DESC
LIMIT 10;


-- 8. Top 10 zones d'arrivée
SELECT
    dropoff_zone,
    COUNT(*) AS trip_count
FROM nyc_taxi_data_platform_dev.curated_trips
GROUP BY dropoff_zone
ORDER BY trip_count DESC
LIMIT 10;


-- 9. Analyse des trajets rejetés
SELECT
    trip_distance,
    trip_duration_minutes,
    total_amount,
    pickup_zone,
    dropoff_zone
FROM nyc_taxi_data_platform_dev.rejected_trips
ORDER BY total_amount DESC
LIMIT 50;