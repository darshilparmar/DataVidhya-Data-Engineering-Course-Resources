-- Fails when a populated PM2.5 concentration is outside the defensible 0 to 1000 ug/m3 teaching range.

select *
from {{ ref('mart_city_air_quality') }}
where pm25_ugm3 is not null
  and (pm25_ugm3 < 0 or pm25_ugm3 > 1000)
