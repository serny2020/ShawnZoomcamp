-- WITH quarterly_revenue AS (
--     -- Compute total revenue per year and quarter for each taxi type
--     SELECT 
--         EXTRACT(YEAR FROM pickup_datetime) AS year,
--         EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
--         'Green Taxi' AS taxi_type,
--         SUM(total_amount) AS revenue
--     FROM {{ ref('stg_green_tripdata') }}
--     WHERE total_amount IS NOT NULL
--         AND pickup_datetime BETWEEN TIMESTAMP('2019-01-01') AND ('2020-12-31')
--     GROUP BY 1, 2

--     UNION ALL

--     SELECT 
--         EXTRACT(YEAR FROM pickup_datetime) AS year,
--         EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
--         'Yellow Taxi' AS taxi_type,
--         SUM(total_amount) AS revenue
--     FROM {{ ref('stg_yellow_tripdata') }}
--     WHERE total_amount IS NOT NULL
--         AND pickup_datetime BETWEEN TIMESTAMP('2019-01-01') AND ('2020-12-31')
--     GROUP BY 1, 2
-- )

-- SELECT * 
-- FROM quarterly_revenue
-- ORDER BY year DESC, quarter DESC, taxi_type

{{ config(materialized='table') }}

with yellow as (
    select
        'Yellow Taxi' as taxi_type,
        cast(extract(year from pickup_datetime) as int64) as trip_year,
        cast(extract(quarter from pickup_datetime) as int64) as quarter,
        total_amount
    from {{ ref('stg_yellow_tripdata') }}
    where total_amount > 0
),
green as (
    select
        'Green Taxi' as taxi_type,
        cast(extract(year from pickup_datetime) as int64) as trip_year,
        cast(extract(quarter from pickup_datetime) as int64) as quarter,
        total_amount
    from {{ ref('stg_green_tripdata') }}
    where total_amount > 0
),

all_trips as (
    select * from yellow
    union all
    select * from green
)

select
    taxi_type,
    trip_year,
    quarter,
    sum(total_amount) as quarterly_revenue
from all_trips
group by taxi_type, trip_year, quarter
order by taxi_type, trip_year, quarter