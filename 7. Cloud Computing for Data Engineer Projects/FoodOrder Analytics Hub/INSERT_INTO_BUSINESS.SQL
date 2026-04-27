INSERT INTO BUSINESS.dim_customers (
    customer_id, first_name, last_name, full_name,
    email, phone, address, city, state, zip_code,
    age_group, customer_segment, loyalty_tier,
    registration_date, first_order_date, last_order_date,
    customer_tenure_days, start_date, current_flag, version_number
)
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    c.email,
    c.phone,
    c.address,
    c.city,
    c.state,
    c.zip_code,

    -- Derived Age Group
    CASE 
        WHEN TIMESTAMPDIFF(YEAR, c.registration_date, CURDATE()) <= 25 THEN '18-25'
        WHEN TIMESTAMPDIFF(YEAR, c.registration_date, CURDATE()) BETWEEN 26 AND 35 THEN '26-35'
        WHEN TIMESTAMPDIFF(YEAR, c.registration_date, CURDATE()) BETWEEN 36 AND 50 THEN '36-50'
        ELSE '50+' 
    END AS age_group,

    -- Customer Segment based on Spending (To be improved later with RFM)
    CASE 
        WHEN SUM(o.total_amount) OVER (PARTITION BY c.customer_id) < 500 THEN 'REGULAR'
        WHEN SUM(o.total_amount) OVER (PARTITION BY c.customer_id) BETWEEN 500 AND 2000 THEN 'GOLD'
        ELSE 'PLATINUM'
    END AS customer_segment,

    -- Loyalty Tier based on Number of Orders
    CASE 
        WHEN COUNT(o.order_id) OVER (PARTITION BY c.customer_id) < 5 THEN 'NEW'
        WHEN COUNT(o.order_id) OVER (PARTITION BY c.customer_id) BETWEEN 5 AND 15 THEN 'LOYAL'
        ELSE 'VIP'
    END AS loyalty_tier,

    c.registration_date,
    MIN(date(o.order_date)) OVER (PARTITION BY c.customer_id) AS first_order_date,
    MAX(date(o.order_date)) OVER (PARTITION BY c.customer_id) AS last_order_date,

    DATEDIFF(CURDATE(), c.registration_date) AS customer_tenure_days,
    CURDATE() AS start_date,
    TRUE AS current_flag,
    1 AS version_number
FROM Stage.stg_customers c
LEFT JOIN Stage.stg_orders o ON c.customer_id = o.customer_id;
select * from BUSINESS.dim_customers;


select * from  BUSINESS.dim_restaurants;
INSERT INTO BUSINESS.dim_restaurants (
    restaurant_id, restaurant_name, cuisine_type,
    city, state, zip_code, phone, email, is_active
)
SELECT 
    restaurant_id,
    restaurant_name,
    cuisine_type,
    city,
    state,
    zip_code,
    phone,
    email,
    is_active
FROM Stage.stg_restaurants
;



INSERT INTO BUSINESS.fact_order_patterns (
    customer_key,
    restaurant_key,
    date_key,
    order_count,
    total_spent,
    avg_basket_size,
    preferred_cuisine,
    meal_period,
    preferred_order_hour,
    preferred_order_day,
    favorite_category,
    dietary_preferences,
    preferred_delivery_address,
    avg_delivery_distance
)
SELECT
    dc.customer_key,
    dr.restaurant_key,
    DATE_FORMAT(o.order_date, '%Y%m%d') AS date_key,
    COUNT(*) AS order_count,
    SUM(o.total_amount) AS total_spent,
    AVG(o.total_amount) AS avg_basket_size,
    dr.cuisine_type AS preferred_cuisine,
    CASE 
        WHEN HOUR(o.order_date) BETWEEN 6 AND 11 THEN 'Breakfast'
        WHEN HOUR(o.order_date) BETWEEN 12 AND 16 THEN 'Lunch'
        WHEN HOUR(o.order_date) BETWEEN 17 AND 21 THEN 'Dinner'
        ELSE 'Late Night'
    END AS meal_period,
    HOUR(o.order_date) AS preferred_order_hour,
    DAYNAME(o.order_date) AS preferred_order_day,
    MAX(o.delivery_address) AS preferred_delivery_address,
    NULL AS avg_delivery_distance
FROM Stage.stg_orders o
JOIN BUSINESS.dim_customers dc ON o.customer_id = dc.customer_id
JOIN BUSINESS.dim_restaurants dr ON o.restaurant_id = dr.restaurant_id
GROUP BY
    dc.customer_key,
    dr.restaurant_key,
    date_key,
    preferred_order_hour,
    preferred_order_day,
    preferred_cuisine,
    meal_period;
