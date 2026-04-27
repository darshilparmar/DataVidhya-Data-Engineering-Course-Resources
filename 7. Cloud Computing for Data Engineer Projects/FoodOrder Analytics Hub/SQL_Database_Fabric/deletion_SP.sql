-- WE need to set the parameter in the pipeline and then we will be able to do it dynamically

DECLARE @audit_id         VARCHAR
DECLARE @load_start_ts    DATETIME2(6);
DECLARE @load_end_ts      DATETIME2(6);
DECLARE @rows_deleted  BIGINT;

-- Capture load start time
SELECT @load_start_ts = CURRENT_TIMESTAMP;

IF '@{pipeline().parameters.p_where}' = ''
BEGIN
    THROW 50002, 'WHERE clause is mandatory in Fabric', 1;
END;

DELETE FROM  @{pipeline().parameters.p_target_schema}.@{pipeline().parameters.p_target_table} 
where @{pipeline().parameters.p_where};
SET @rows_deleted = @@ROWCOUNT;
-- Capture load end time
SELECT @load_end_ts = CURRENT_TIMESTAMP;	
Select @audit_id = CONCAT(
    'job_load_',
    FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd_HHmmss'));
INSERT INTO Stage.etl_load_audit (
    audit_id,
    job_name,
    source_schema,
    source_table,
    target_schema,
    target_table,
    load_start_time,
    load_end_time,
    records_deleted,
    load_status,
    executed_by,
    execution_date
)
VALUES (
    @audit_id,
    'deletion_LOAD',
    'Stage',
    'stg_orders',
    'Stage',
    'fact_order_patterns',
    @load_start_ts,
    @load_end_ts,
    @rows_deleted,
    'SUCCESS',
    'ETL_LOAD',
    CAST(@load_end_ts AS DATE)
);
