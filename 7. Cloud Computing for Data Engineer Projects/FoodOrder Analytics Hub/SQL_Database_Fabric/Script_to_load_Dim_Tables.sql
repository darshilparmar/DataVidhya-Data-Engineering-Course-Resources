-- Declare variables
DECLARE @row_count_before BIGINT;
DECLARE @row_count_after  BIGINT;
DECLARE @load_start_ts    DATETIME2(6);
DECLARE @load_end_ts      DATETIME2(6);
DECLARE @audit_id         VARCHAR;


-- Capture load start time
SELECT @load_start_ts = CURRENT_TIMESTAMP;

-- Count before load
SELECT @row_count_before = COUNT(*)
FROM business.dim_customers;

-- Load data
INSERT INTO business.dim_customers (
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
    WHEN
        DATEDIFF(YEAR, c.registration_date, CAST(GETDATE() AS DATE))
        - CASE
            WHEN DATEADD(
                    YEAR,
                    DATEDIFF(YEAR, c.registration_date, CAST(GETDATE() AS DATE)),
                    c.registration_date
                 ) > CAST(GETDATE() AS DATE)
            THEN 1 ELSE 0
          END <= 25
    THEN '18-25'

    WHEN
        DATEDIFF(YEAR, c.registration_date, CAST(GETDATE() AS DATE))
        - CASE
            WHEN DATEADD(
                    YEAR,
                    DATEDIFF(YEAR, c.registration_date, CAST(GETDATE() AS DATE)),
                    c.registration_date
                 ) > CAST(GETDATE() AS DATE)
            THEN 1 ELSE 0
          END BETWEEN 26 AND 35
    THEN '26-35'

    WHEN
        DATEDIFF(YEAR, c.registration_date, CAST(GETDATE() AS DATE))
        - CASE
            WHEN DATEADD(
                    YEAR,
                    DATEDIFF(YEAR, c.registration_date, CAST(GETDATE() AS DATE)),
                    c.registration_date
                 ) > CAST(GETDATE() AS DATE)
            THEN 1 ELSE 0
          END BETWEEN 36 AND 50
    THEN '36-50'

    ELSE '50+'
END
,

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
        MIN(CAST(o.order_date AS DATE)) OVER (PARTITION BY c.customer_id),
        MAX(CAST(o.order_date AS DATE)) OVER (PARTITION BY c.customer_id),
        DATEDIFF(DAY, c.registration_date, CAST(GETDATE() AS DATE)),
        CAST(GETDATE() AS DATE),
        1,
        1
    FROM Stage.stg_customers c
    LEFT JOIN Stage.stg_orders o
        ON c.customer_id = o.customer_id;

-- Count after load
SELECT @row_count_after = COUNT(*)
FROM business.dim_customers;

-- Capture load end time
SELECT @load_end_ts = CURRENT_TIMESTAMP;

-- Select @audit_id = CONCAT(
    'job_load_',
    FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd_HHmmss'));

-- Insert audit record
INSERT INTO Stage.etl_load_audit (
    audit_id,
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
    @audit_id,
    'JOB_stg_customers_to_dim_customers_LOAD',
    'Stage',
    'stg_customers',
    'Stage',
    'dim_customers',
    @load_start_ts,
    @load_end_ts,
    @row_count_after - @row_count_before,
    'SUCCESS',
    'ETL_LOAD',
    CAST(@load_end_ts AS DATE)
);


-- Capture load start time
SELECT @load_start_ts = CURRENT_TIMESTAMP;

-- Count before load
SELECT @row_count_before = COUNT(*)
FROM business.dim_restaurants;

--Load Data
   INSERT INTO business.dim_restaurants (
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
	
-- Count after load
SELECT @row_count_after = COUNT(*)
FROM business.dim_restaurants;

-- Capture load end time
SELECT @load_end_ts = CURRENT_TIMESTAMP;	

-- Select @audit_id = CONCAT(
    'job_load_',
    FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd_HHmmss'));

-- Insert audit record
INSERT INTO Stage.etl_load_audit (
    audit_id,
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
    @audit_id,
    'JOB_stg_restaurants_to_dim_restaurants_LOAD',
    'Stage',
    'stg_restaurants',
    'Stage',
    'dim_restaurants',
    @load_start_ts,
    @load_end_ts,
    @row_count_after - @row_count_before,
    'SUCCESS',
    'ETL_LOAD',
    CAST(@load_end_ts AS DATE)
);
