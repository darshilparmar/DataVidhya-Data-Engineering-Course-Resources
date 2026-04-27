Firstly we will load the Ingestion Table in KQL database and this table will be having all the Realtime JSON Data.



CREATE SCHEMA STAGE;

SELECT * FROM STAGE.STG_CUSTOMER;

CREATE TABLE STAGE.STG_CUSTOMER
(
    CUSTOMER_ID         VARCHAR(50),
    FULL_NAME           VARCHAR(200),
    DATE_OF_BIRTH       DATE,
    GENDER              VARCHAR(20),
    CITY                VARCHAR(100),
    STATE               VARCHAR(100),
    PINCODE             VARCHAR(20),
    EMPLOYMENT_TYPE     VARCHAR(50),
    ANNUAL_INCOME       DECIMAL(18,2),
    ACCOUNT_OPEN_DATE   DATE,
    KYC_STATUS          VARCHAR(20),      
    CUSTOMER_STATUS     VARCHAR(20),     
    INGESTION_TS        DATETIME2
);


CREATE TABLE STAGE.STG_LOAN_APPLICATION
(
    APPLICATION_ID      VARCHAR(100),
    CUSTOMER_ID         VARCHAR(100),
    APPLICATION_TS      DATETIME2(6),
    CHANNEL             VARCHAR(50),      
    LOAN_PRODUCT        VARCHAR(50),      
    REQUESTED_AMOUNT    DECIMAL(18,2),
    TENURE_MONTHS       INT,
    INTEREST_RATE       DECIMAL(5,2),
    PURPOSE             VARCHAR(200),
    BRANCH_ID           VARCHAR(50),
    APPLICATION_STATUS  VARCHAR(50),      
    INGESTION_TS        DATETIME2(6)
);



CREATE TABLE STAGE.STG_APPLICATION_DEVICE
(
    APPLICATION_ID      VARCHAR(100),
    DEVICE_ID           VARCHAR(100),
    DEVICE_TYPE         VARCHAR(100),      
    OS_VERSION          VARCHAR(100),
    BROWSER             VARCHAR(100),
    IP_ADDRESS          VARCHAR(100),
    LATITUDE            DECIMAL(5,2),
    LONGITUDE           DECIMAL(5,2),
    EVENT_TS            DATETIME2(6),
    INGESTION_TS        DATETIME2(6)
);



CREATE TABLE STAGE.STG_CREDIT_BUREAU
(
    CUSTOMER_ID         VARCHAR(100),
    CREDIT_SCORE        INT,
    TOTAL_ACCOUNTS      INT,
    ACTIVE_LOANS        INT,
    PAST_DEFAULTS       INT,
    ENQUIRIES_LAST_6M   INT,
    BUREAU_SOURCE       VARCHAR(100),
    REPORT_DATE         DATE
);
