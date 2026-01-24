#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Check if data exists in MinIO for a given execution date.
This script is used in an Airflow DAG to verify that the batch_data_consumer.py script produced data.
"""

import logging
import sys
from datetime import datetime

from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "stock-market-data"
MINIO_ENDPOINT = "minio:9000"

def create_minio_client():
    """Initialize MinIO Client."""
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        logger.info(f"MinIO client initialized. Endpoint: {MINIO_ENDPOINT}, Bucket: {MINIO_BUCKET}")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize MinIO client: {e}")
        raise

def check_data_in_minio(minio_client, execution_date):
    """
    Check if data exists in MinIO for the given execution date.
    
    Args:
        minio_client: MinIO client instance
        execution_date: Execution date in YYYY-MM-DD format
    
    Returns:
        bool: True if data exists, False otherwise
    """
    logger.info(f"\n------ Checking Data in MinIO for {execution_date} ------")
    
    # Construct the prefix for the expected data location
    year, month, day = execution_date.split("-")
    s3_prefix = f"raw/historical/year={year}/month={month}/day={day}/"
    logger.info(f"Checking data in s3://{MINIO_BUCKET}/{s3_prefix}")
    
    try:
        # List objects in the specified prefix
        objects = minio_client.list_objects(
            MINIO_BUCKET,
            prefix=s3_prefix,
            recursive=True
        )
        
        # Check if any objects exist
        object_list = list(objects)
        if not object_list:
            logger.error(f"No data found in MinIO for {s3_prefix}")
            return False
        
        # Log the found objects
        logger.info(f"Found {len(object_list)} objects in MinIO for {execution_date}:")
        for obj in object_list:
            logger.info(f" - {obj.object_name}")
        
        return True
    
    except S3Error as e:
        logger.error(f"Error checking data in MinIO: {e}")
        return False

def main():
    """Main function to check if data exists in MinIO."""
    logger.info("\n=========================================")
    logger.info("STARTING MINIO DATA CHECK")
    logger.info("=========================================\n")
    
    # Get execution date from command-line argument
    if len(sys.argv) != 2:
        execution_date = datetime.now().strftime("%Y-%m-%d")
        logger.warning(f"Execution date not provided, defaulting to today: {execution_date}")
    else:
        execution_date = sys.argv[1]
    
    logger.info(f"Checking data for execution date: {execution_date}")
    
    # Initialize MinIO client
    minio_client = create_minio_client()
    
    # Check if data exists in MinIO
    data_exists = check_data_in_minio(minio_client, execution_date)
    
    if not data_exists:
        logger.error("Data check failed: No data found in MinIO")
        sys.exit(1)  # Exit with error code to fail the Airflow task
    
    logger.info("\nData check passed: Data found in MinIO")
    logger.info("\n=========================================")
    logger.info("MINIO DATA CHECK COMPLETED")
    logger.info("=========================================")

if __name__ == "__main__":
    main()