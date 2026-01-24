#Total revenue by vendor

SELECT v.vendor_id, SUM(f.total_revenue) AS revenue
FROM fact_trips f
JOIN dim_vendor v ON f.vendor_key = v.vendor_key
GROUP BY v.vendor_id
ORDER BY revenue DESC;

#Daily average fare trend
SELECT f.trip_date, AVG(f.avg_fare) AS avg_daily_fare
FROM facts f
GROUP BY f.trip_date
ORDER BY f.trip_date;

#Revenue by payment type
SELECT p.payment_type, SUM(f.total_revenue) AS revenue
FROM facts f
JOIN dim_payment p ON f.payment_key = p.payment_key
GROUP BY p.payment_type
ORDER BY revenue DESC;


#Top 10 Pickup → Dropoff Routes by Revenue
SELECT l.pickup_location,
       l.dropoff_location,
       SUM(f.total_revenue) AS revenue,
       SUM(f.total_trips) AS trips
FROM facts f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.pickup_location, l.dropoff_location
ORDER BY revenue DESC
LIMIT 10;

