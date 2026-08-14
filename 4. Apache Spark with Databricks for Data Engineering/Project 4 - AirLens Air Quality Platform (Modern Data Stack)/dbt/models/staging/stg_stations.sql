-- Produces a typed station registry view so OpenAQ metadata stays separate from pollutant measurements.

select
    cast(query_city as string) as query_city,
    cast(station_id as bigint) as station_id,
    cast(station_name as string) as station_name,
    cast(country_code as string) as country_code,
    cast(station_timezone as string) as station_timezone,
    cast(provider as string) as provider,
    cast(is_reference_monitor as boolean) as is_reference_monitor,
    cast(lat as double) as lat,
    cast(lon as double) as lon
from {{ source('airlens_core', 'silver_stations') }}
