-- Produces one station-coverage row per queried city so registry breadth can be monitored independently of readings.

select
    query_city as city,
    count(distinct station_id) as station_count,
    count(distinct case when is_reference_monitor then station_id end) as reference_monitor_count,
    count(distinct provider) as provider_count,
    count(distinct case when lat is not null and lon is not null then station_id end) as geocoded_station_count
from {{ ref('stg_stations') }}
group by query_city
