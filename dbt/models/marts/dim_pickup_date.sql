{{ config(
    materialized='table',
    unique_key='pickup_date'
) }}

with distinct_pickup_dates as (
    select distinct
        date_trunc('day', pickup_datetime)::date as pickup_date
    from {{ ref('stg_yellow_tripdata') }}
),

calendar_enriched as (
    select
        pickup_date,
        extract(year from pickup_date)::int                    as pickup_year,
        extract(quarter from pickup_date)::int                 as pickup_quarter,
        extract(month from pickup_date)::int                   as pickup_month,
        to_char(pickup_date, 'Month')                          as pickup_month_name,
        extract(week from pickup_date)::int                    as pickup_week,
        extract(isodow from pickup_date)::int                  as pickup_weekday_number,
        to_char(pickup_date, 'Day')                            as pickup_weekday_name,
        case when extract(isodow from pickup_date) in (6, 7)
            then true else false end                          as is_weekend,
        date_trunc('month', pickup_date)::date                 as pickup_month_start,
        (date_trunc('month', pickup_date) + interval '1 month' - interval '1 day')::date as pickup_month_end
    from distinct_pickup_dates
)

select *
from calendar_enriched
order by pickup_date
