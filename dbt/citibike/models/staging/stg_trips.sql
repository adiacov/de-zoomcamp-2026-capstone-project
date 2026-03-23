
with raw as (
    select * from {{source("de_citibike_raw", "trips")}}
),

filtered as (
    select *
    from raw
    where started_at >= CAST('2024-01-01' AS TIMESTAMP) -- the dataset in this project starts from January 2024
),

renamed as (
    select
        ride_id,
        rideable_type,
        started_at as ride_start_time,
        ended_at as ride_end_time,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat as ride_start_lat,
        start_lng as ride_start_lng,
        end_lat as ride_end_lat,
        end_lng as ride_end_lng,
        member_casual as customer_type,
        _loaded_at as loaded_at
    from filtered
),

derived as (
    select
        *,
        TIMESTAMP_DIFF(ride_end_time, ride_start_time, SECOND) as ride_duration_seconds,
        (start_station_id is not null) as pickup_from_station, -- ride started from a station
        (end_station_id is not null) as drop_to_station -- ride ended at a station
    from renamed
),

deduped as (
    select
        ride_id,
        rideable_type,
        ride_start_time,
        ride_end_time,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        ride_start_lat,
        ride_start_lng,
        ride_end_lat,
        ride_end_lng,
        customer_type,
        ride_duration_seconds,
        pickup_from_station,
        drop_to_station,
        loaded_at
    from (
        select
            *,
            row_number() over (partition by ride_id order by ride_start_time desc, loaded_at desc) as row_num
        from derived
    ) where row_num = 1 -- newest row by ride_start_time wins
)

select * from deduped