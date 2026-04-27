-- Declare variable
DECLARE @row_count_before BIGINT;
DECLARE @row_count_after  BIGINT;
DECLARE @load_start_ts    DATETIME2(6);
DECLARE @load_end_ts      DATETIME2(6);
DECLARE @audit_id         BIGINT;


-- Capture load start time
SELECT @load_start_ts = CURRENT_TIMESTAMP;

-- Count before load
SELECT @row_count_before = COUNT(*)
FROM Stage.stg_customers;

-- Load data
INSERT INTO Stage.stg_customers (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    zip_code,
    registration_date
)
VALUES (
    1001,
    'john',
    'smith',
    'john.smith@gmail.com',
    '555-123-4567',
    '123 main st',
    'new york',
    'NY',
    '10001',
    '2023-01-15'
);

-- Count after load
SELECT @row_count_after = COUNT(*)
FROM Stage.stg_customers;

-- Capture load end time
SELECT @load_end_ts = CURRENT_TIMESTAMP;

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
    1,
    'JOB_stg_customers_LOAD',
    'Stage',
    'stg_customers',
    'Stage',
    'stg_customers',
    @load_start_ts,
    @load_end_ts,
    @row_count_after - @row_count_before,
    'SUCCESS',
    'ETL_LOAD',
    CAST(@load_end_ts AS DATE)
);
