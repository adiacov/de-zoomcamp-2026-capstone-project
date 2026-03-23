
with source as (
    select * from {{ ref("stg_trips") }}
),

transformed as (
    select
        customer_type,
        rideable_type,
        case 
            when (extract(dayofweek from ride_start_time) in (2, 3, 4, 5, 6)) then  'weekday'
            else  'weekend'
        end as day_type
    from source
    where ride_start_time is not null
),

aggregated as (
    select
        day_type,
        customer_type,
        rideable_type,
        count(*) as trip_count
    from transformed
    group by day_type, customer_type, rideable_type
)

select * from aggregated