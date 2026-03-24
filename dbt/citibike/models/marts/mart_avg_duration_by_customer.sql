-- Grain: avg trip duration by customer_type


with source as (
    select * from {{ ref("stg_trips") }}
),

windowed as (
    select 
        customer_type,
        rideable_type,
        ride_duration_seconds,
        percentile_cont(ride_duration_seconds, 0.5)
            over(partition by customer_type, rideable_type) as ride_duration_median
    from source
    where ride_duration_seconds is not null
),

result as (
    select
        customer_type,
        rideable_type,
        cast(avg(ride_duration_seconds) as int) as avg_trip_duration_seconds,
        cast(avg(ride_duration_median) as int) as median_trip_duration_seconds
    from windowed
    group by customer_type, rideable_type
),

final as (
    select
        customer_type,
        rideable_type,
        round(avg_trip_duration_seconds / 60, 2) as avg_trip_duration_minutes,
        round(median_trip_duration_seconds / 60, 2) as median_trip_duration_minutes
    from result
)

select * from final