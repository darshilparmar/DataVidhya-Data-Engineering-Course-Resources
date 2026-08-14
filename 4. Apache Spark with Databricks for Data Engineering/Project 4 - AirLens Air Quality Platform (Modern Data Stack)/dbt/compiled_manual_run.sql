-- AirLens gold layer, compiled from the dbt project into plain Databricks SQL.
-- Same logic as dbt/models/**, with ref() and source() resolved to real names.
-- Paste into a Databricks SQL editor and use "Run all". Safe to re-run.

-- ─────────────────────────────────────────────────────────────
-- 0. Target schema for everything dbt would have built
-- ─────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS airlens.analytics;

-- ─────────────────────────────────────────────────────────────
-- 1. Staging views (thin, typed contracts over silver)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW airlens.analytics.stg_air_quality AS
SELECT
    CAST(city AS STRING)                AS city,
    CAST(source AS STRING)              AS source,
    CAST(measured_at_utc AS TIMESTAMP)  AS measured_at_utc,
    CAST(fetched_at_utc AS TIMESTAMP)   AS fetched_at_utc,
    CAST(pm25_ugm3 AS DOUBLE)           AS pm25_ugm3,
    CAST(pm10_ugm3 AS DOUBLE)           AS pm10_ugm3,
    CAST(o3_ugm3 AS DOUBLE)             AS o3_ugm3,
    CAST(no2_ugm3 AS DOUBLE)            AS no2_ugm3,
    CAST(aqi_value AS DOUBLE)           AS aqi_value,
    CAST(aqi_scale AS STRING)           AS aqi_scale,
    CAST(station_name AS STRING)        AS station_name
FROM airlens.core.silver_aq_readings;

CREATE OR REPLACE VIEW airlens.analytics.stg_weather AS
SELECT
    CAST(city AS STRING)                AS city,
    CAST(measured_at_utc AS TIMESTAMP)  AS measured_at_utc,
    CAST(temp_c AS DOUBLE)              AS temp_c,
    CAST(humidity_pct AS DOUBLE)        AS humidity_pct,
    CAST(wind_ms AS DOUBLE)             AS wind_ms,
    CAST(conditions AS STRING)          AS conditions
FROM airlens.core.silver_weather;

CREATE OR REPLACE VIEW airlens.analytics.stg_stations AS
SELECT
    CAST(query_city AS STRING)          AS query_city,
    CAST(station_id AS BIGINT)          AS station_id,
    CAST(station_name AS STRING)        AS station_name,
    CAST(country_code AS STRING)        AS country_code,
    CAST(station_timezone AS STRING)    AS station_timezone,
    CAST(provider AS STRING)            AS provider,
    CAST(is_reference_monitor AS BOOLEAN) AS is_reference_monitor,
    CAST(lat AS DOUBLE)                 AS lat,
    CAST(lon AS DOUBLE)                 AS lon
FROM airlens.core.silver_stations;

-- ─────────────────────────────────────────────────────────────
-- 2. Gold: one row per city, source, and measurement hour.
--    aqi_scale travels with every row so nothing can average
--    OpenWeather's 1 to 5 against WAQI's 0 to 500.
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE airlens.analytics.mart_city_air_quality AS
WITH hourly_air AS (
    SELECT
        city,
        source,
        aqi_scale,
        date_trunc('hour', measured_at_utc) AS measured_at_utc,
        avg(aqi_value)   AS aqi_value,
        avg(pm25_ugm3)   AS pm25_ugm3,
        avg(pm10_ugm3)   AS pm10_ugm3,
        avg(o3_ugm3)     AS o3_ugm3,
        avg(no2_ugm3)    AS no2_ugm3,
        max(station_name)   AS station_name,
        max(fetched_at_utc) AS latest_fetch_at_utc
    FROM airlens.analytics.stg_air_quality
    GROUP BY city, source, aqi_scale, date_trunc('hour', measured_at_utc)
),
hourly_weather AS (
    SELECT
        city,
        date_trunc('hour', measured_at_utc) AS measured_at_utc,
        avg(temp_c)       AS temp_c,
        avg(humidity_pct) AS humidity_pct,
        avg(wind_ms)      AS wind_ms,
        max(conditions)   AS conditions
    FROM airlens.analytics.stg_weather
    GROUP BY city, date_trunc('hour', measured_at_utc)
)
SELECT
    concat(a.city, '|', a.source, '|', CAST(a.measured_at_utc AS STRING)) AS city_source_measurement_hour,
    a.city,
    a.source,
    a.aqi_scale,
    a.measured_at_utc,
    a.aqi_value,
    a.pm25_ugm3,
    a.pm10_ugm3,
    a.o3_ugm3,
    a.no2_ugm3,
    a.station_name,
    a.latest_fetch_at_utc,
    w.temp_c,
    w.humidity_pct,
    w.wind_ms,
    w.conditions,
    -- EPA 2024 PM2.5 breakpoints. A single snapshot is a descriptive band,
    -- not a regulatory 24-hour AQI.
    CASE
        WHEN a.pm25_ugm3 IS NULL   THEN 'not_available'
        WHEN a.pm25_ugm3 <= 9.0    THEN 'good'
        WHEN a.pm25_ugm3 <= 35.4   THEN 'moderate'
        WHEN a.pm25_ugm3 <= 55.4   THEN 'unhealthy_for_sensitive_groups'
        WHEN a.pm25_ugm3 <= 125.4  THEN 'unhealthy'
        WHEN a.pm25_ugm3 <= 225.4  THEN 'very_unhealthy'
        ELSE 'hazardous'
    END AS pm25_category,
    -- WHO 2021 24-hour guidance is 15 ug/m3, exposed as a transparent reference.
    CASE WHEN a.pm25_ugm3 IS NULL THEN NULL ELSE a.pm25_ugm3 <= 15.0 END AS within_who_24h_guideline
FROM hourly_air a
LEFT JOIN hourly_weather w
    ON a.city = w.city
   AND a.measured_at_utc = w.measured_at_utc;

-- ─────────────────────────────────────────────────────────────
-- 3. Gold: daily grain per city, source, scale.
--    In dbt this is an incremental merge model. Built here as a
--    full refresh, which is the honest choice at this data size.
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE airlens.analytics.mart_city_daily AS
SELECT
    concat(city, '|', source, '|', CAST(CAST(measured_at_utc AS DATE) AS STRING)) AS city_source_date,
    city,
    source,
    aqi_scale,
    CAST(measured_at_utc AS DATE) AS measurement_date,
    avg(aqi_value)  AS average_aqi_value,
    avg(pm25_ugm3)  AS average_pm25_ugm3,
    avg(pm10_ugm3)  AS average_pm10_ugm3,
    avg(o3_ugm3)    AS average_o3_ugm3,
    avg(no2_ugm3)   AS average_no2_ugm3,
    avg(temp_c)     AS average_temp_c,
    count(*)        AS measurement_hours
FROM airlens.analytics.mart_city_air_quality
GROUP BY city, source, aqi_scale, CAST(measured_at_utc AS DATE);

-- ─────────────────────────────────────────────────────────────
-- 4. Gold: station registry coverage per queried city
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE airlens.analytics.mart_station_coverage AS
SELECT
    query_city AS city,
    count(DISTINCT station_id) AS station_count,
    count(DISTINCT CASE WHEN is_reference_monitor THEN station_id END) AS reference_monitor_count,
    count(DISTINCT provider) AS provider_count,
    count(DISTINCT CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN station_id END) AS geocoded_station_count
FROM airlens.analytics.stg_stations
GROUP BY query_city;

-- ─────────────────────────────────────────────────────────────
-- 5. The data tests. Each MUST return 0 rows.
--    Any rows returned means the pipeline shipped bad data.
-- ─────────────────────────────────────────────────────────────

-- Test 1: no source may carry another provider's AQI scale
SELECT 'assert_aqi_scale_not_mixed' AS failing_test, count(*) AS failing_rows
FROM airlens.analytics.mart_city_air_quality
WHERE (source = 'openweather' AND aqi_scale <> 'openweather-1-5')
   OR (source = 'waqi'        AND aqi_scale <> 'us-epa-0-500')
   OR source NOT IN ('openweather', 'waqi');

-- Test 2: PM2.5 must be inside a defensible range when populated
SELECT 'assert_pm25_in_range' AS failing_test, count(*) AS failing_rows
FROM airlens.analytics.mart_city_air_quality
WHERE pm25_ugm3 IS NOT NULL
  AND (pm25_ugm3 < 0 OR pm25_ugm3 > 1000);

-- Test 3: the daily grain must be unique
SELECT 'unique_city_source_date' AS failing_test, count(*) AS failing_rows
FROM (
    SELECT city_source_date
    FROM airlens.analytics.mart_city_daily
    GROUP BY city_source_date
    HAVING count(*) > 1
);

-- ─────────────────────────────────────────────────────────────
-- 6. Results worth screenshotting
-- ─────────────────────────────────────────────────────────────

-- Row counts per gold table
SELECT 'mart_city_air_quality' AS table_name, count(*) AS rows FROM airlens.analytics.mart_city_air_quality
UNION ALL
SELECT 'mart_city_daily',       count(*) FROM airlens.analytics.mart_city_daily
UNION ALL
SELECT 'mart_station_coverage', count(*) FROM airlens.analytics.mart_station_coverage;

-- Dirtiest cities by PM2.5, with the scale kept explicit
SELECT city, source, aqi_scale, measurement_date,
       round(average_pm25_ugm3, 2) AS avg_pm25_ugm3,
       round(average_aqi_value, 1) AS avg_aqi
FROM airlens.analytics.mart_city_daily
ORDER BY average_pm25_ugm3 DESC NULLS LAST
LIMIT 15;

-- The teaching money shot: same city, same hour, two incomparable AQI numbers
SELECT ow.city,
       ow.aqi_value AS openweather_aqi_1_to_5,
       wq.aqi_value AS waqi_aqi_epa_0_to_500,
       round(ow.pm25_ugm3, 1) AS openweather_pm25_ugm3,
       wq.station_name        AS waqi_station
FROM airlens.analytics.mart_city_air_quality ow
JOIN airlens.analytics.mart_city_air_quality wq
  ON ow.city = wq.city
 AND ow.source = 'openweather'
 AND wq.source = 'waqi'
ORDER BY wq.aqi_value DESC
LIMIT 12;
