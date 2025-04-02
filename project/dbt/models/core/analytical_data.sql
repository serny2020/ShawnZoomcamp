{{
    config(
        materialized='table'
    )
}}

with accidents_stg as (
    select * from {{ ref('stg_us_accidents') }}
),

cleaned as (
    select
        id,
        source,
        SAFE_CAST(severity as integer) as severity,
        SAFE_CAST(start_time as timestamp) as start_time,
        SAFE_CAST(end_time as timestamp) as end_time,
        SAFE_CAST(start_lat as numeric) as start_lat,
        SAFE_CAST(start_lng as numeric) as start_lng,
        SAFE_CAST(end_lat as numeric) as end_lat,
        SAFE_CAST(end_lng as numeric) as end_lng,
        SAFE_CAST(distance_mi as numeric) as distance_mi,
        description,
        street,
        city,
        county,
        state,
        zipcode,
        country,
        timezone,
        airport_code,
        SAFE_CAST(weather_timestamp as timestamp) as weather_timestamp,
        SAFE_CAST(temperature_f as numeric) as temperature_f,
        SAFE_CAST(wind_chill_f as numeric) as wind_chill_f,
        SAFE_CAST(humidity_pct as numeric) as humidity_pct,
        SAFE_CAST(pressure_in as numeric) as pressure_in,
        SAFE_CAST(visibility_mi as numeric) as visibility_mi,
        -- Convert wind_direction from cardinal values to degrees if needed
        case 
            when lower(cast(wind_direction as string)) = 'n' then 0
            when lower(cast(wind_direction as string)) = 'nne' then 22.5
            when lower(cast(wind_direction as string)) = 'ne' then 45
            when lower(cast(wind_direction as string)) = 'ene' then 67.5
            when lower(cast(wind_direction as string)) = 'e' then 90
            when lower(cast(wind_direction as string)) = 'ese' then 112.5
            when lower(cast(wind_direction as string)) = 'se' then 135
            when lower(cast(wind_direction as string)) = 'sse' then 157.5
            when lower(cast(wind_direction as string)) = 's' then 180
            when lower(cast(wind_direction as string)) = 'ssw' then 202.5
            when lower(cast(wind_direction as string)) = 'sw' then 225
            when lower(cast(wind_direction as string)) = 'wsw' then 247.5
            when lower(cast(wind_direction as string)) = 'w' then 270
            when lower(cast(wind_direction as string)) = 'wnw' then 292.5
            when lower(cast(wind_direction as string)) = 'nw' then 315
            when lower(cast(wind_direction as string)) = 'nnw' then 337.5
            else SAFE_CAST(wind_direction as numeric)
        end as wind_direction,
        SAFE_CAST(wind_speed_mph as numeric) as wind_speed_mph,
        SAFE_CAST(precipitation_in as numeric) as precipitation_in,
        weather_condition,
        -- Convert possible string/integer booleans to true boolean values
        case 
            when lower(cast(amenity as string)) in ('true', '1') then true 
            else false 
        end as amenity,
        case 
            when lower(cast(bump as string)) in ('true', '1') then true 
            else false 
        end as bump,
        case 
            when lower(cast(crossing as string)) in ('true', '1') then true 
            else false 
        end as crossing,
        case 
            when lower(cast(give_way as string)) in ('true', '1') then true 
            else false 
        end as give_way,
        case 
            when lower(cast(junction as string)) in ('true', '1') then true 
            else false 
        end as junction,
        case 
            when lower(cast(no_exit as string)) in ('true', '1') then true 
            else false 
        end as no_exit,
        case 
            when lower(cast(railway as string)) in ('true', '1') then true 
            else false 
        end as railway,
        case 
            when lower(cast(roundabout as string)) in ('true', '1') then true 
            else false 
        end as roundabout,
        case 
            when lower(cast(station as string)) in ('true', '1') then true 
            else false 
        end as station,
        case 
            when lower(cast(stop as string)) in ('true', '1') then true 
            else false 
        end as stop,
        case 
            when lower(cast(traffic_calming as string)) in ('true', '1') then true 
            else false 
        end as traffic_calming,
        case 
            when lower(cast(traffic_signal as string)) in ('true', '1') then true 
            else false 
        end as traffic_signal,
        case 
            when lower(cast(turning_loop as string)) in ('true', '1') then true 
            else false 
        end as turning_loop,
        sunrise_sunset,
        civil_twilight,
        nautical_twilight,
        astronomical_twilight,
        -- Derived columns for analysis
        DATE(SAFE_CAST(start_time as timestamp)) as accident_date,
        TIMESTAMP_TRUNC(SAFE_CAST(start_time as timestamp), HOUR) as accident_hour,
        TIMESTAMP_DIFF(SAFE_CAST(end_time as timestamp), SAFE_CAST(start_time as timestamp), SECOND)/60 as duration_minutes,
        -- Flag for adverse weather conditions based on keywords
        case
            when lower(weather_condition) like '%rain%' 
              or lower(weather_condition) like '%snow%' 
              or lower(weather_condition) like '%fog%' 
              or lower(weather_condition) like '%storm%' then true
            else false
        end as adverse_weather
    from accidents_stg
)

select * from cleaned