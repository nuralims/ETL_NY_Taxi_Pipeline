

with trips as (

    select *
    from "ny_taxi"."public_stg"."stg_yellow_tripdata"

),

cleaned as (

    select
        trip_id,
        vendor_id,

        pickup_datetime,
        dropoff_datetime,
        date_trunc('day', pickup_datetime)::date as pickup_date,

        passenger_count,
        trip_distance,
        rate_code_id,
        store_and_fwd_flag,

        pickup_location_id,
        dropoff_location_id,

        payment_type,
        case
            when payment_type = 1 then 'Credit card'
            when payment_type = 2 then 'Cash'
            when payment_type = 3 then 'No charge'
            when payment_type = 4 then 'Dispute'
            when payment_type = 5 then 'Unknown'
            when payment_type = 6 then 'Voided trip'
            else 'Other'
        end as payment_type_desc,

        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        total_amount,

        trip_duration_minutes,

        -- contoh: gross revenue (tanpa mta_tax, tergantung kebutuhan)
        (fare_amount
         + extra
         + tip_amount
         + tolls_amount
         + improvement_surcharge
         + congestion_surcharge
         + airport_fee
         + cbd_congestion_fee) as gross_revenue

    from trips
    where
        total_amount > 0
        and trip_distance > 0
        and trip_duration_minutes > 0
)

select *
from cleaned