# Databricks notebook source
# MAGIC %md
# MAGIC # AirLens: silver layer, harmonizing three APIs
# MAGIC Three sources use three vocabularies. OpenWeather scores air quality from 1 to
# MAGIC 5, while WAQI reports the US EPA index from 0 to 500. WAQI pollutant values are
# MAGIC sub-indexes, not concentrations. Silver makes units, scales, and timestamps
# MAGIC explicit so downstream models cannot silently mix them.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Three WAQI status=nope responses stored an error string in payload.data.
# MAGIC -- Parse the JSON text only for status=ok rows.
# MAGIC SELECT
# MAGIC   ow.city,
# MAGIC   ow.payload.pollution.list[0].main.aqi AS openweather_aqi_1_to_5,
# MAGIC   CAST(get_json_object(wq.payload.data, '$.aqi') AS INT) AS waqi_aqi_epa_0_to_500
# MAGIC FROM airlens.core.bronze_openweather ow
# MAGIC JOIN airlens.core.bronze_waqi wq USING (city)
# MAGIC WHERE wq.payload.status = 'ok'
# MAGIC ORDER BY waqi_aqi_epa_0_to_500 DESC
# MAGIC LIMIT 8

# COMMAND ----------

# MAGIC %md
# MAGIC Bengaluru returned an OpenWeather value of 1 next to a WAQI value of 160 in
# MAGIC the verified run. Both APIs call the field `aqi`, so every downstream table
# MAGIC carries `aqi_scale` explicitly.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE airlens.core.silver_cities AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('Delhi','IN','Asia/Kolkata'), ('Mumbai','IN','Asia/Kolkata'),
# MAGIC   ('Bengaluru','IN','Asia/Kolkata'), ('Kolkata','IN','Asia/Kolkata'),
# MAGIC   ('Chennai','IN','Asia/Kolkata'), ('Hyderabad','IN','Asia/Kolkata'),
# MAGIC   ('Beijing','CN','Asia/Shanghai'), ('Shanghai','CN','Asia/Shanghai'),
# MAGIC   ('Tokyo','JP','Asia/Tokyo'), ('Jakarta','ID','Asia/Jakarta'),
# MAGIC   ('Bangkok','TH','Asia/Bangkok'), ('Dubai','AE','Asia/Dubai'),
# MAGIC   ('London','GB','Europe/London'), ('Paris','FR','Europe/Paris'),
# MAGIC   ('Berlin','DE','Europe/Berlin'), ('New York','US','America/New_York'),
# MAGIC   ('Los Angeles','US','America/Los_Angeles'), ('Mexico City','MX','America/Mexico_City'),
# MAGIC   ('Sao Paulo','BR','America/Sao_Paulo'), ('Lagos','NG','Africa/Lagos')
# MAGIC AS t(city, country, timezone)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OpenWeather rows carry concentrations. WAQI rows carry only its EPA AQI.
# MAGIC CREATE OR REPLACE TABLE airlens.core.silver_aq_readings AS
# MAGIC SELECT
# MAGIC   city,
# MAGIC   'openweather' AS source,
# MAGIC   CAST(payload.pollution.list[0].dt AS TIMESTAMP) AS measured_at_utc,
# MAGIC   CAST(fetched_at_utc AS TIMESTAMP) AS fetched_at_utc,
# MAGIC   payload.pollution.list[0].components.pm2_5 AS pm25_ugm3,
# MAGIC   payload.pollution.list[0].components.pm10 AS pm10_ugm3,
# MAGIC   payload.pollution.list[0].components.o3 AS o3_ugm3,
# MAGIC   payload.pollution.list[0].components.no2 AS no2_ugm3,
# MAGIC   CAST(payload.pollution.list[0].main.aqi AS INT) AS aqi_value,
# MAGIC   'openweather-1-5' AS aqi_scale,
# MAGIC   NULL AS station_name
# MAGIC FROM airlens.core.bronze_openweather
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   city,
# MAGIC   'waqi',
# MAGIC   to_utc_timestamp(
# MAGIC     get_json_object(payload.data, '$.time.s'),
# MAGIC     get_json_object(payload.data, '$.time.tz')
# MAGIC   ),
# MAGIC   CAST(fetched_at_utc AS TIMESTAMP),
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   NULL,
# MAGIC   CAST(get_json_object(payload.data, '$.aqi') AS INT),
# MAGIC   'us-epa-0-500',
# MAGIC   get_json_object(payload.data, '$.city.name')
# MAGIC FROM airlens.core.bronze_waqi
# MAGIC WHERE payload.status = 'ok';
# MAGIC
# MAGIC SELECT source, aqi_scale, COUNT(*) AS readings
# MAGIC FROM airlens.core.silver_aq_readings
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE airlens.core.silver_weather AS
# MAGIC SELECT
# MAGIC   city,
# MAGIC   CAST(payload.weather.dt AS TIMESTAMP) AS measured_at_utc,
# MAGIC   payload.weather.main.temp AS temp_c,
# MAGIC   payload.weather.main.humidity AS humidity_pct,
# MAGIC   payload.weather.wind.speed AS wind_ms,
# MAGIC   payload.weather.weather[0].main AS conditions
# MAGIC FROM airlens.core.bronze_openweather;
# MAGIC
# MAGIC SELECT COUNT(*) AS weather_rows FROM airlens.core.silver_weather

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OpenAQ returned a station registry, not pollutant readings.
# MAGIC CREATE OR REPLACE TABLE airlens.core.silver_stations AS
# MAGIC SELECT
# MAGIC   city AS query_city,
# MAGIC   station.id AS station_id,
# MAGIC   station.name AS station_name,
# MAGIC   station.country.code AS country_code,
# MAGIC   station.timezone AS station_timezone,
# MAGIC   station.provider.name AS provider,
# MAGIC   station.isMonitor AS is_reference_monitor,
# MAGIC   station.coordinates.latitude AS lat,
# MAGIC   station.coordinates.longitude AS lon
# MAGIC FROM airlens.core.bronze_openaq
# MAGIC LATERAL VIEW explode(payload.results) AS station;
# MAGIC
# MAGIC SELECT COUNT(*) AS stations, COUNT(DISTINCT query_city) AS cities
# MAGIC FROM airlens.core.silver_stations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.city, r.source, r.aqi_value, r.aqi_scale, r.pm25_ugm3,
# MAGIC        r.measured_at_utc, c.timezone,
# MAGIC        from_utc_timestamp(r.measured_at_utc, c.timezone) AS measured_at_local
# MAGIC FROM airlens.core.silver_aq_readings r
# MAGIC JOIN airlens.core.silver_cities c USING (city)
# MAGIC WHERE r.city = 'Delhi'
