-- Top 10 stations start/end
-- station_role start: where do customers begin the commute
-- station_role end: where do customers end the commute


with source as (
    select * from {{ ref("stg_trips") }}
),

start_stations as (
    select
        start_station_name as station_name,
        'start' as station_role,
        count(*) as trip_count
    from source
    where start_station_name is not null
    group by start_station_name
    order by trip_count desc
    limit 10
),

end_stations as (
    select 
        end_station_name as station_name,
        'end' as station_role,
        count(*) as trip_count
    from source
    where end_station_name is not null
    group by end_station_name
    order by trip_count desc 
    limit 10
),

result as (
    select * from start_stations
    union all
    select * from end_stations
)

select * from result