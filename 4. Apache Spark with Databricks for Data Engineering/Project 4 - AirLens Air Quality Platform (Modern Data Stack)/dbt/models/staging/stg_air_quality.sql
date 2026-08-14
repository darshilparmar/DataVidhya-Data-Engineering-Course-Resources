-- Produces a typed, lightly renamed view over silver air quality readings for stable downstream contracts.

select
    cast(city as string) as city,
    cast(source as string) as source,
    cast(measured_at_utc as timestamp) as measured_at_utc,
    cast(fetched_at_utc as timestamp) as fetched_at_utc,
    cast(pm25_ugm3 as double) as pm25_ugm3,
    cast(pm10_ugm3 as double) as pm10_ugm3,
    cast(o3_ugm3 as double) as o3_ugm3,
    cast(no2_ugm3 as double) as no2_ugm3,
    cast(aqi_value as double) as aqi_value,
    cast(aqi_scale as string) as aqi_scale,
    cast(station_name as string) as station_name
from {{ source('airlens_core', 'silver_aq_readings') }}
