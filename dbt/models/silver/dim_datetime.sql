{{ config(
    materialized='table',
    tags=['dimension']
) }}

with dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2018-10-01' as date)",
        end_date="cast('2025-03-31' as date)"
    )
    }}
),

hours as (
    select 
        hour_number
    from unnest(sequence(0, 23)) as hour_number
),

base_dates as (
    select 
        cast(date_day as timestamp) as date_day
    from dates
),

date_hours as (
    select 
        date_add(date_day, interval hour_number hour) as datetime
    from base_dates
    cross join hours
)

select
    unix_timestamp(datetime) as dateKey,
    datetime,
    dayofweek(datetime) as dayOfWeek,
    dayofmonth(datetime) as dayOfMonth,
    dayofyear(datetime) as dayOfYear,
    month(datetime) as month,
    year(datetime) as year,
    case when dayofweek(datetime) in (6, 7) then True else False end as weekendFlag
from date_hours
order by datetime 