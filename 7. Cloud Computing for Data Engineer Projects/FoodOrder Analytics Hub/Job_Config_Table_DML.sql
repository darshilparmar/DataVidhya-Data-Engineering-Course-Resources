CREATE TABLE etl_load_audit (
    audit_id BIGINT NOT NULL AUTO_INCREMENT,
    job_name VARCHAR(100),
    source_schema VARCHAR(50),
    source_table VARCHAR(100),
    target_schema VARCHAR(50),
    target_table VARCHAR(100),
    load_start_time DATETIME,
    load_end_time DATETIME,
    records_inserted INT,
    records_updated INT,
    records_deleted INT,
    load_status VARCHAR(20),
    error_message TEXT,
    executed_by VARCHAR(100),
    execution_date DATE,
    PRIMARY KEY (audit_id)
);
select * from etl_load_audit;

    load_status VARCHAR(20),
    error_message TEXT,

    executed_by VARCHAR(100),
    execution_date DATE DEFAULT CURRENT_DATE
);


