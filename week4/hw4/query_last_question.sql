select * from {{ ref('fct_fhv_monthly_zone_traveltime_p90') }}
select * from {{ ref('dim_zones') }}



SELECT 
    pickup_zone,
    dropoff_zone,
    trip_year,
    trip_month,
    p90_trip_duration
FROM {{ ref('fct_fhv_monthly_zone_traveltime_p90') }}
WHERE pickup_zone = 'SoHo'
  AND trip_year = 2019
  AND trip_month = 11
ORDER BY p90_trip_duration DESC
