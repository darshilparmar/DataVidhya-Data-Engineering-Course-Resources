DECLARE @v_date DATE = '2020-01-01';
DECLARE @end_date DATE = '2025-12-31';

WHILE @v_date <= @end_date
BEGIN
    INSERT INTO BUSINESS.dim_date (
        date_key,
        full_date,
        [day],
        [month],
        [year],
        [week],
        day_name
    )
    VALUES (
        CONVERT(INT, FORMAT(@v_date, 'yyyyMMdd')),
        @v_date,
        DAY(@v_date),
        MONTH(@v_date),
        YEAR(@v_date),
        DATEPART(ISO_WEEK, @v_date),
        DATENAME(WEEKDAY, @v_date)
    );

    SET @v_date = DATEADD(DAY, 1, @v_date);
END;
