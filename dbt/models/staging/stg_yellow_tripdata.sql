{{ config(
    materialized = 'view'
) }}

with source as (

    select
        *
    from {{ source('ny_taxi', 'raw_yellow_taxi_data') }}

),

renamed as (

    select
        -- surrogate key trip
        md5(
            concat_ws(
                '-',
                vendorid::text,
                tpep_pickup_datetime::text,
                tpep_dropoff_datetime::text
            )
        ) as trip_id,

        vendorid                         as vendor_id,
        tpep_pickup_datetime             as pickup_datetime,
        tpep_dropoff_datetime            as dropoff_datetime,
        passenger_count::int             as passenger_count,
        trip_distance::float             as trip_distance,
        ratecodeid                       as rate_code_id,

        case
            when store_and_fwd_flag = 'Y' then true
            when store_and_fwd_flag = 'N' then false
            else null
        end                              as store_and_fwd_flag,

        pulocationid                     as pickup_location_id,
        dolocationid                     as dropoff_location_id,

        payment_type                     as payment_type,
        fare_amount::numeric             as fare_amount,
        extra::numeric                   as extra,
        mta_tax::numeric                 as mta_tax,
        tip_amount::numeric              as tip_amount,
        tolls_amount::numeric            as tolls_amount,
        improvement_surcharge::numeric   as improvement_surcharge,
        total_amount::numeric            as total_amount,
        congestion_surcharge::numeric    as congestion_surcharge,
        airport_fee::numeric             as airport_fee,
        cbd_congestion_fee::numeric      as cbd_congestion_fee,

        -- durasi perjalanan dalam menit
        extract(
            epoch from (tpep_dropoff_datetime - tpep_pickup_datetime)
        ) / 60.0                         as trip_duration_minutes

    from source
)

select *
from renamed
