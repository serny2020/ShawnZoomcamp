{{
    config(
        materialized='table'
    )
}}

with partitioned_data as (
    select * from {{ ref('int_accidents_partitioned') }}
)

select
    state,
    accident_date,
    count(*) as total_accidents,
    avg(severity) as avg_severity,
    sum(case when adverse_weather then 1 else 0 end) as adverse_weather_count
from partitioned_data
group by state, accident_date
order by accident_date, state