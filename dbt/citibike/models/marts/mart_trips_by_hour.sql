
with source as (
    select * from {{ ref("stg_trips") }}
),

result as (
    select
        date(ride_start_time) as trip_date,
        extract(hour from ride_start_time) as trip_hour,
        customer_type,
        rideable_type,
        count(*) as trip_count
    from source
    where ride_start_time is not null
    group by trip_date, trip_hour, customer_type, rideable_type
)

select * from result