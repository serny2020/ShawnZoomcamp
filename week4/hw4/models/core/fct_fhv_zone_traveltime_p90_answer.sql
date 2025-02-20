{{ config(materialized='table') }}

WITH data AS (
    SELECT
        trip_year,
        trip_month,
        pickup_zone,
        dropoff_zone,
        p90_trip_duration
    FROM {{ ref('fct_fhv_monthly_zone_traveltime_p90') }}
    WHERE trip_year = 2019
      AND trip_month = 11
      AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
),

ranked AS (
    SELECT
        pickup_zone,
        dropoff_zone,
        p90_trip_duration,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_zone
            ORDER BY p90_trip_duration DESC
        ) AS rn
    FROM data
)

SELECT
    pickup_zone,
    dropoff_zone,
    p90_trip_duration
FROM ranked
WHERE rn = 2
ORDER BY pickup_zone