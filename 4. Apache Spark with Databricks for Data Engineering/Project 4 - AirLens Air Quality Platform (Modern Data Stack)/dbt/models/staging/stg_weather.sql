-- Produces a typed view over silver weather because marts need a stable hourly join surface, not another stored copy.

select
    cast(city as string) as city,
    cast(measured_at_utc as timestamp) as measured_at_utc,
    cast(temp_c as double) as temp_c,
    cast(humidity_pct as double) as humidity_pct,
    cast(wind_ms as double) as wind_ms,
    cast(conditions as string) as conditions
from {{ source('airlens_core', 'silver_weather') }}
