{{ config(materialized='table') }}

with filtered_trips as (
    select
        service_type,  -- e.g., "Green Taxi" or "Yellow Taxi"
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    -- from {{ ref('fact_trips') }}
    from {{ ref('fact_trips') }}
    where fare_amount > 0
    --   and trip_distance > 0
    --   and payment_type_description in ('Cash', 'Credit Card')
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
