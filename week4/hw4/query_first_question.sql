{{ config(materialized='view') }}

WITH ranked_revenue AS (
    SELECT 
        taxi_type,  -- Green Taxi or Yellow Taxi
        quarter,
        quarterly_revenue,
        RANK() OVER (PARTITION BY taxi_type ORDER BY quarterly_revenue DESC) AS best_rank,
        RANK() OVER (PARTITION BY taxi_type ORDER BY quarterly_revenue ASC) AS worst_rank
    FROM {{ ref('fct_taxi_trips_quarterly_revenue') }}
    WHERE trip_year = 2020
)

SELECT 
    taxi_type,
    MAX(CASE WHEN best_rank = 1 THEN quarter END) AS best_quarter,
    MAX(CASE WHEN best_rank = 1 THEN quarterly_revenue END) AS best_quarter_revenue,
    MAX(CASE WHEN worst_rank = 1 THEN quarter END) AS worst_quarter,
    MAX(CASE WHEN worst_rank = 1 THEN quarterly_revenue END) AS worst_quarter_revenue
FROM ranked_revenue
GROUP BY taxi_type