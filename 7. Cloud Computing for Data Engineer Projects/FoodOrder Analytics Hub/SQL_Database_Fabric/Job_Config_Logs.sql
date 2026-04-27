CREATE  TABLE Stage.etl_load_audit (
    audit_id VARCHAR(MAX),
    job_name VARCHAR(100),
    source_schema VARCHAR(50),
    source_table VARCHAR(100),
    target_schema VARCHAR(50),
    target_table VARCHAR(100),

    load_start_time DATETIME2(6),
    load_end_time   DATETIME2(6),

    records_inserted BIGINT,
    records_updated  BIGINT,
    records_deleted  BIGINT,

    load_status VARCHAR(20),
    error_message VARCHAR(4000),

    executed_by VARCHAR(100),
    execution_date DATE
);
