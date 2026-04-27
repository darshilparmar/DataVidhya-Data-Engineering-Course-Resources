CALL execute_delete_statement(
  'BUSINESS',
  'fact_order_patterns',
  'DATE_KEY < 20230101'
);


DROP PROCEDURE execute_delete_statement;

DELIMITER $$

CREATE PROCEDURE execute_delete_statement (
    IN p_target_schema VARCHAR(64),
    IN p_target_table  VARCHAR(64),
    IN p_where_clause  TEXT
)
BEGIN
    DECLARE v_load_start_time DATETIME;
    DECLARE v_rows_deleted INT;
    DECLARE v_sql TEXT;

    -- Mandatory validations
    IF p_target_table IS NULL OR p_target_table = '' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Target table is mandatory';
    END IF;

    IF p_where_clause IS NULL OR TRIM(p_where_clause) = '' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'WHERE clause is mandatory for DELETE';
    END IF;

    SET v_load_start_time = NOW();

    -- Build DELETE SQL
    SET v_sql = CONCAT(
        'DELETE FROM ',
        p_target_schema, '.', p_target_table,
        ' WHERE ',
        p_where_clause
    );

    -- Execute dynamic SQL
    SET @sql_text = v_sql;
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    SET v_rows_deleted = ROW_COUNT();
    DEALLOCATE PREPARE stmt;

    -- Audit logging
    INSERT INTO Stage.etl_load_audit (
        job_name,
        source_schema,
        source_table,
        target_schema,
        target_table,
        load_start_time,
        load_end_time,
        records_updated,
        load_status,
        executed_by,
        execution_date
    )
    VALUES (
        'DELETION_LOAD',
        'Stage',
        'NA',
        p_target_schema,
        p_target_table,
        v_load_start_time,
        NOW(),
        v_rows_deleted,
        'SUCCESS',
        'ETL_LOAD',
        CURDATE()
    );
END$$

DELIMITER ;


