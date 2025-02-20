select count(*) from {{ ref('fact_fhv_trips') }}
select count(*) from {{ ref('fact_trips') }}

-- 2024 script
with green_yellow as (
    select service_type, count(*) as total_records
    from {{ ref('fact_trips') }}
    where extract(year from pickup_datetime) = 2019
    and extract(month from pickup_datetime) = 7
    group by 1
),

fhv as (
    select service_type, count(*) as total_records
    from {{ ref('fact_fhv_trips') }}
    where extract(year from pickup_datetime) = 2019
    and extract(month from pickup_datetime) = 7
    group by 1
)

select * from green_yellow
union all
select * from fhv



-- test on service type
select service_type, count(*)
from {{ ref('fact_trips') }}
where extract(year from pickup_datetime) = 2019
and extract(month from pickup_datetime) = 7

