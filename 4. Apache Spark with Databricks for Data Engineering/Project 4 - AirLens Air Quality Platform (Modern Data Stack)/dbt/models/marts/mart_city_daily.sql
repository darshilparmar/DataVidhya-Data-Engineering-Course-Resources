-- Produces one incremental daily row per city, source, and AQI scale for efficient recurring refreshes.

{{ config(
    materialized='incremental',
    unique_key='city_source_date',
    incremental_strategy='merge'
) }}

with hourly as (
    select *
    from {{ ref('mart_city_air_quality') }}
    {% if is_incremental() %}
    -- Incremental merge pays off as history grows. For this 37-row teaching snapshot, a full refresh is simpler and equally reasonable.
    where measured_at_utc >= (
        select coalesce(max(measurement_date), cast('1900-01-01' as date))
        from {{ this }}
    )
    {% endif %}
)

select
    concat(city, '|', source, '|', cast(cast(measured_at_utc as date) as string)) as city_source_date,
    city,
    source,
    aqi_scale,
    cast(measured_at_utc as date) as measurement_date,
    avg(aqi_value) as average_aqi_value,
    avg(pm25_ugm3) as average_pm25_ugm3,
    avg(pm10_ugm3) as average_pm10_ugm3,
    avg(o3_ugm3) as average_o3_ugm3,
    avg(no2_ugm3) as average_no2_ugm3,
    avg(temp_c) as average_temp_c,
    count(*) as measurement_hours
from hourly
group by city, source, aqi_scale, cast(measured_at_utc as date)
