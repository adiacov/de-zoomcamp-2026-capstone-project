-- Grain: trip count by year / by month / by customer_type / by rideable_type


with source as (
    select * from {{ ref("stg_trips") }}
),

result as (
    select
        extract(year from ride_start_time) as year,
        extract(month from ride_start_time) as month,
        customer_type,
        rideable_type,
        count(*) as trip_count
    from source
    where ride_start_time is not null
    group by year, month, customer_type, rideable_type
)

select * from result