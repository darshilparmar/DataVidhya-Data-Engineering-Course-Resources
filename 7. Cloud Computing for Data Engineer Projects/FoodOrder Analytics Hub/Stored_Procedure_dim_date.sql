call load_dim_date();

DELIMITER $$

CREATE PROCEDURE load_dim_date()
BEGIN
    DECLARE v_date DATE;

    -- Start date
    SET v_date = '2020-01-01';

    WHILE v_date <= '2025-12-31' DO

        INSERT INTO BUSINESS.dim_date (
            date_key,
            full_date,
            day,
            month,
            year,
            week,
            day_name
        )
        VALUES (
            DATE_FORMAT(v_date, '%Y%m%d'),
            v_date,
            DAY(v_date),
            MONTH(v_date),
            YEAR(v_date),
            WEEK(v_date, 1),
            DAYNAME(v_date)
        );

        -- Next day
        SET v_date = DATE_ADD(v_date, INTERVAL 1 DAY);

    END WHILE;

END$$

DELIMITER ;
