{{ config(materialized='table') }}

WITH trips AS (
    SELECT
        *,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration,
        EXTRACT(YEAR FROM pickup_datetime) AS trip_year,
        EXTRACT(MONTH FROM pickup_datetime) AS trip_month
    FROM {{ ref('dim_fhv_trips') }}
)

SELECT
    trip_year,
    trip_month,
    pickup_locationid,
    pickup_zone,
    dropoff_locationid,
    dropoff_zone,
    APPROX_QUANTILES(trip_duration, 101)[OFFSET(90)] AS p90_trip_duration
FROM trips
GROUP BY 
    trip_year, 
    trip_month, 
    pickup_locationid, 
    pickup_zone, 
    dropoff_locationid, 
    dropoff_zone
ORDER BY 
    trip_year, 
    trip_month, 
    pickup_locationid, 
    dropoff_locationid
