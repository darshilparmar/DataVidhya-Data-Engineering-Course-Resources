import logging
import sys
import traceback
from datetime import datetime, timedelta

import boto3
import numpy as np

import pandas as pd

import snowflake.connector
import io 

SNOWFLAKE_PASSWORD = ''

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# S3/MinIO configuration
S3_ENDPOINT = 'http://localhost:9000'  # Adjust if MinIO is not accessible at this endpoint
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
S3_BUCKET = 'stock-market-data'

#Snowflake Config
SNOWFLAKE_ACCOUNT = 'VPZRLQT-TT86916'
SNOWFLAKE_USER = 'DATAVIDHYA123'
SNOWFLAKE_DATABASE = "STOCKMARKETBATCH"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = "DAILY_STOCK_METRICS"

def init_s3_client():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY
        )
        logger.info(f"S3 client initialized")
        return s3_client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        raise

def init_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        logger.info(f"Snowflake connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to establish Snowflake connection: {e}")
        raise

def create_snowflake_table(conn):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
    symbol STRING,
    date DATE,
    daily_open FLOAT,
    daily_high FLOAT,
    daily_low FLOAT,
    daily_volume FLOAT,
    daily_close FLOAT,
    daily_change FLOAT,
    last_updated TIMESTAMP,
    PRIMARY KEY (symbol, date)
    )
    """

    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logger.info(f"Snowflake table created")
    except Exception as e:
        logger.error(f"Failed to create Snowflake table: {e}")
        raise
    finally:
        cursor.close()

def read_processed_data(s3_client, execution_date):
    logger.info(f"\n------ Raading Processed Data-------")

    s3_prefix = f"processed/historical/date={execution_date}"
    logger.info(f"Reading data from s3://{S3_BUCKET}/{s3_prefix}")

    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
        if "Contents" not in response:
            logger.info(f"No data found for execution date {execution_date}")
            return None
        
        dfs = []

        for obj in response['Contents']:
            if obj['Key'].endswith(".parquet"):
                logger.info(f"Reading file: {obj['Key']}")
                try:
                    symbol = None
                    parts = obj['Key'].split("/")
                    for part in parts:
                        if part.startswith("symbol="):
                            symbol = part.split("=")[-1]
                            break

                    if not symbol:
                        logger.error(f"Failed to extract symbol from key: {obj['Key']}")
                        continue

                    response = s3_client.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                    parquet_data = response['Body'].read()

                    parquet_buffer = io.BytesIO(parquet_data)
                    df = pd.read_parquet(parquet_buffer)

                    if "symbol" not in df.columns:
                        df['symbol'] = symbol
                    
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"Failed to read file: {obj['Key']}: {e}")
                    continue

        if not dfs:
            logger.info(f"No data found for execution date {execution_date}")
            return None
        
        processed_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Read {len(processed_df)} rows from S3")

        if "date" in processed_df.columns:
            processed_df['date'] = pd.to_datetime(processed_df['date']).dt.date
        else:
            logger.error("Missing 'date' column in processed data")
            return None
        
        processed_df['late_updated'] = datetime.now()
        processed_df = processed_df.drop_duplicates(subset=['symbol','date'], keep='last')


        required_columns = {
            "symbol": "symbol",
            "date": "date",
            "daily_open": "daily_open",
            "daily_high": "daily_high",
            "daily_low": "daily_low",
            "daily_volume": "daily_volume",
            "daily_close": "daily_close",
            "daily_change":"daily_change"
        }
        processed_df = processed_df[list(required_columns.keys())]

        return processed_df
    
    except Exception as e:
        logger.error(f"Failed to read data from S3: {e}")
        return None

def incremental_load_to_snowflake(conn, df):
    logger.info("\n----------- Performing Incremental Load into Snowflake")

    if df is None or df.empty:
        logger.info("No data to load")
        return
    
    try:
        # Create a new session
        cursor = conn.cursor()

        stage_table = "TEMP_STAGE_TABLE"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} LIKE {SNOWFLAKE_TABLE}")

        records = df.to_records(index=False)
        columns = list(df.columns)
        placeholder = ",".join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {stage_table} ({','.join(columns)}) VALUES ({placeholder})"
        logger.info(f"Executing INSERT QUERY: {insert_query}")

        record_list = []
        for record in records:
            record_tuple = tuple(
                None if pd.isna(val) else
                val.item() if isinstance(val, (pd.Timestamp, pd._libs.tslibs.nattype.NaTType)) else
                float(val) if isinstance(val, (np.floating, np.float64)) else
                int(val) if isinstance(val, (np.integer, np.int64)) else
                val 
                for val in record
            )
            record_list.append(record_tuple)
        
        logger.info(f"Prepared {len(record_list)} records for insertion")
        logger.info(f"Sample record: {record_list}")

        cursor.executemany(insert_query, record_list)
        
        merge_query = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} As target
        USING {stage_table} as source
        ON target.symbol = source.symbol AND target.date = source.date
        WHEN MATCHED THEN
            UPDATE SET
                target.daily_open = source.daily_open,
                target.daily_high = source.daily_high,
                target.daily_low = source.daily_low,
                target.daily_volume = source.daily_volume,
                target.daily_close = source.daily_close,
                target.daily_change = source.daily_change,
                target.last_updated = source.last_updated
        WHEN NOT MATCHED THEN
            INSERT (symbol,date,daily_open,daily_high,daily_low,daily_volume,daily_close,daily_change, last_updated)
            VALUES (source.symbol,source.date,source.daily_open, source.daily_high, source.daily_low, source.daily_volume, source.daily_close, source.daily_change, source.last_updated)

        """

        cursor.execute(merge_query)
        logger.info(f"Successfully performed incremental load")

    except Exception as e:
        logger.error(f"Failed to create temporary table: {e}")
        return
    finally:
        cursor.close()


def main():
    logger.info("\n=========================================")
    logger.info("STARTING SNOWFLAKE INCREMENTAL LOAD")
    logger.info("=========================================\n")

    execution_date = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d")

    s3_client = init_s3_client()

    conn = init_snowflake_connection()

    try:
        create_snowflake_table(conn)

        df = read_processed_data(s3_client, execution_date)

        if df is not None:
            incremental_load_to_snowflake(conn, df)

        else:
            logger.info("\nNo data to load into Snowflake")
    except Exception as e:
        logger.error(f"Failed to excute commads on Snowflake: {e}")
        sys.exit(1)
    finally:
        conn.close()
        logger.info("Snowflake connection closed")



if __name__ == "__main__":
    main()
