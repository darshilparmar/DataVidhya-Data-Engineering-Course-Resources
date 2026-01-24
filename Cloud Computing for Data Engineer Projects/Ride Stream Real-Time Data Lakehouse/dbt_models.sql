#sources for now it is aqi_db but we need to change this

version: 2

sources:
  - name: taxi_source
    database: awsdatacatalog   # the catalog (always this in Athena)
    schema: aqi_db             # your Glue database
    tables:
      - name: facts            # your Glue table name


#Total revenue by vendor
#Firstly create the file and put the query :
vi total_revenue.sql

{{ config(materialized='table') }}

select
    v.vendor_id,
    sum(f.total_revenue) as revenue
from {{ source('raw', 'facts') }} f
join {{ source('raw', 'dim_vendor') }} v
    on f.vendor_key = v.vendor_key
group by v.vendor_id
order by revenue desc


dbt run --select total_revenue


#Daily average fare trend


{{ config(materialized='table') }}

SELECT f.trip_date, AVG(f.avg_fare) AS avg_daily_fare
FROM from {{ source('raw', 'facts') }} f
GROUP BY f.trip_date
ORDER BY f.trip_date;


#Revenue by payment type


{{ config(materialized='table') }}
SELECT p.payment_type, SUM(f.total_revenue) AS revenue
FROM {{ source('raw', 'facts') }} f
JOIN {{ source('raw', 'dim_payment') }} p ON f.payment_key = p.payment_key
GROUP BY p.payment_type
ORDER BY revenue DESC;

#Top 10 Pickup to Dropoff Routes by Revenue

{{ config(materialized='table') }}
SELECT l.pickup_location,
       l.dropoff_location,
       SUM(f.total_revenue) AS revenue,
       SUM(f.total_trips) AS trips
FROM {{ source('raw', 'facts') }} f
JOIN {{ source('raw', 'dim_location') }} l ON f.location_key = l.location_key
GROUP BY l.pickup_location, l.dropoff_location
ORDER BY revenue DESC
LIMIT 10;