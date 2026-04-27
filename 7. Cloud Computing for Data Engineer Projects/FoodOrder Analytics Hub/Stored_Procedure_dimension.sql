use business;

drop procedure dimensions_load;
call dimensions_load();
 
 DELIMITER $$

CREATE PROCEDURE dimensions_load()
BEGIN
    -- =========================
    -- Declare variables FIRST
    -- =========================
    DECLARE v_load_start_time DATETIME;
    DECLARE v_count_inserted INT DEFAULT 0;

    -- Capture load start time
    SET v_load_start_time = NOW();

    -- =========================
    -- Load dim_customers
    -- =========================
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
        CONCAT(c.first_name, ' ', c.last_name),
        c.email,
        c.phone,
        c.address,
        c.city,
        c.state,
        c.zip_code,

        CASE 
            WHEN TIMESTAMPDIFF(YEAR, c.registration_date, CURDATE()) <= 25 THEN '18-25'
            WHEN TIMESTAMPDIFF(YEAR, c.registration_date, CURDATE()) BETWEEN 26 AND 35 THEN '26-35'
            WHEN TIMESTAMPDIFF(YEAR, c.registration_date, CURDATE()) BETWEEN 36 AND 50 THEN '36-50'
            ELSE '50+' 
        END,

        CASE 
            WHEN SUM(o.total_amount) OVER (PARTITION BY c.customer_id) < 500 THEN 'REGULAR'
            WHEN SUM(o.total_amount) OVER (PARTITION BY c.customer_id) BETWEEN 500 AND 2000 THEN 'GOLD'
            ELSE 'PLATINUM'
        END,

        CASE 
            WHEN COUNT(o.order_id) OVER (PARTITION BY c.customer_id) < 5 THEN 'NEW'
            WHEN COUNT(o.order_id) OVER (PARTITION BY c.customer_id) BETWEEN 5 AND 15 THEN 'LOYAL'
            ELSE 'VIP'
        END,

        c.registration_date,
        MIN(DATE(o.order_date)) OVER (PARTITION BY c.customer_id),
        MAX(DATE(o.order_date)) OVER (PARTITION BY c.customer_id),
        DATEDIFF(CURDATE(), c.registration_date),
        CURDATE(),
        TRUE,
        1
    FROM Stage.stg_customers c
    LEFT JOIN Stage.stg_orders o
        ON c.customer_id = o.customer_id;

    -- Capture rows inserted
    SET v_count_inserted = ROW_COUNT();

    -- =========================
    -- Audit log for dim_customers
    -- =========================
    INSERT INTO Stage.etl_load_audit (
        job_name,
        source_schema,
        source_table,
        target_schema,
        target_table,
        load_start_time,
        load_end_time,
        records_inserted,
        load_status,
        executed_by,
        execution_date
    )
    VALUES (
        'JOB_stg_orders_TO_dim_customers_LOAD',
        'Stage',
        'stg_orders',
        'BUSINESS',
        'dim_customers',
        v_load_start_time,
        NOW(),
        v_count_inserted,
        'SUCCESS',
        'ETL_LOAD',
        CURDATE()
    );


   SET v_load_start_time = NOW();
    -- =========================
    -- Load dim_restaurants
    -- =========================
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
    FROM Stage.stg_restaurants;
    
    SET v_count_inserted = ROW_COUNT();
    

    -- =========================
    -- Audit log for dim_restaurants
    -- =========================
   INSERT INTO Stage.etl_load_audit (
        job_name,
        source_schema,
        source_table,
        target_schema,
        target_table,
        load_start_time,
        load_end_time,
        records_inserted,
        load_status,
        executed_by,
        execution_date
    )
    VALUES (
        'JOB_stg_restaurants_TO_dim_restaurants_LOAD',
        'Stage',
        'stg_restaurants',
        'BUSINESS',
        'dim_restaurants',
        v_load_start_time,
        NOW(),
        v_count_inserted,
        'SUCCESS',
        'ETL_LOAD',
        CURDATE()
    );

END$$

DELIMITER ;



