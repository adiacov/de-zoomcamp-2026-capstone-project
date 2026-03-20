
with raw as (
    select * from {{source("de_citibike_raw", "trips")}}
),

filtered as (
    select *
    from raw
    where started_at >= CAST('2024-01-01' AS TIMESTAMP) -- the dataset in this project starts from January 2024
),

transformed as (
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
        TIMESTAMP_DIFF(ended_at, started_at, SECOND) as ride_duration_seconds,
        (start_station_id is not null) as pickup_from_station, -- ride started from a station
        (end_station_id is not null) as drop_to_station -- ride ended at a station
    from filtered
)
select * from transformed