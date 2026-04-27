CALL execute_update_statement(
  'BUSINESS',
  'fact_order_patterns',
  'TOTAL_SPENT = 300',
  'DATE_KEY = 20240115'
);


DROP PROCEDURE execute_update_statement;

DELIMITER $$

CREATE PROCEDURE execute_update_statement (
    IN p_target_schema VARCHAR(64),
    IN p_target_table  VARCHAR(64),
    IN p_set_clause    TEXT,
    IN p_where_clause  TEXT
)
BEGIN
    DECLARE v_load_start_time DATETIME;
    DECLARE v_rows_updated INT;
    DECLARE v_sql TEXT;

    -- Safety checks
    IF p_target_table IS NULL OR p_target_table = '' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Target table is mandatory';
    END IF;

    IF p_set_clause IS NULL OR p_set_clause = '' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'SET clause cannot be empty';
    END IF;

    SET v_load_start_time = NOW();

    -- Construct SQL safely
    SET v_sql = CONCAT(
        'UPDATE ',
        p_target_schema, '.', p_target_table,
        ' SET ',
        p_set_clause,
        ' WHERE ',
        p_where_clause
    );

    -- Execute dynamic SQL
    SET @sql_text = v_sql;
    PREPARE stmt FROM @sql_text;
    EXECUTE stmt;
    SET v_rows_updated = ROW_COUNT();
    DEALLOCATE PREPARE stmt;

    -- Audit log
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
        'UPDATION_LOAD',
        'Stage',
        'NA',
        p_target_schema,
        p_target_table,
        v_load_start_time,
        NOW(),
        v_rows_updated,
        'SUCCESS',
        'ETL_LOAD',
        CURDATE()
    );
END$$

DELIMITER ;



