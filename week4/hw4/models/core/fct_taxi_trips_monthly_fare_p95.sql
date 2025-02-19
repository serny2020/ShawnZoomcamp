{{ config(materialized='table') }}

with filtered_trips as (
    select
        service_type,  -- e.g., "Green Taxi" or "Yellow Taxi"
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    from {{ ref('fact_trips') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit Card')
      and extract(year from pickup_datetime) = 2020
      and extract(month from pickup_datetime) = 4
)

select 
    service_type,
    year,
    month,
    FORMAT('%.1f%%', APPROX_QUANTILES(fare_amount, 101)[SAFE_OFFSET(90)]) as fare_p90,  -- 90th percentile
    FORMAT('%.1f%%', APPROX_QUANTILES(fare_amount, 101)[SAFE_OFFSET(95)]) as fare_p95,  -- 95th percentile
    FORMAT('%.1f%%', APPROX_QUANTILES(fare_amount, 101)[SAFE_OFFSET(97)]) as fare_p97   -- 97th percentile
from filtered_trips
group by service_type, year, month
order by year desc, month desc, service_type





-- WITH filtered_trips AS (
--     -- Filter out invalid trips
--     SELECT 
--         'Green Taxi' AS taxi_type,  -- Assign Green Taxi label
--         EXTRACT(YEAR FROM pickup_datetime) AS year,
--         EXTRACT(MONTH FROM pickup_datetime) AS month,
--         fare_amount
--     FROM {{ ref('stg_green_tripdata') }}
--     WHERE fare_amount > 0
--         AND trip_distance > 0
--         AND payment_type_description IN ('Cash', 'Credit Card')

--     UNION ALL

--     SELECT 
--         'Yellow Taxi' AS taxi_type,  -- Assign Yellow Taxi label
--         EXTRACT(YEAR FROM pickup_datetime) AS year,
--         EXTRACT(MONTH FROM pickup_datetime) AS month,
--         fare_amount
--     FROM {{ ref('stg_yellow_tripdata') }}
--     WHERE fare_amount > 0
--         AND trip_distance > 0
--         AND payment_type_description IN ('Cash', 'Credit Card')
-- )

-- SELECT 
--     taxi_type,
--     year,
--     month,
--     FORMAT('%.1f%%', APPROX_QUANTILES(fare_amount, 101)[SAFE_OFFSET(90)]) AS fare_p90,  -- 90th percentile
--     FORMAT('%.1f%%', APPROX_QUANTILES(fare_amount, 101)[SAFE_OFFSET(95)]) AS fare_p95,  -- 95th percentile
--     FORMAT('%.1f%%', APPROX_QUANTILES(fare_amount, 101)[SAFE_OFFSET(97)]) AS fare_p97   -- 97th percentile
-- FROM filtered_trips
-- GROUP BY taxi_type, year, month
-- ORDER BY year DESC, month DESC, taxi_type