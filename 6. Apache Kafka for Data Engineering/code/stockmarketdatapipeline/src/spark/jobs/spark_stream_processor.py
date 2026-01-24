#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark Streaming Processor for Real-Time Stock Data

This script uses Spark Structured Streaming to process real-time stock market data
stored in MinIO/S3. It calculates metrics like moving averages and volatility over
sliding windows and writes the results back to MinIO/S3.
"""

import logging
import os
import sys
import traceback
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Ensures logs go to stdout (visible in Docker logs)
    ]
)
logger = logging.getLogger(__name__)

# S3/MinIO configuration
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "stock-market-data"
MINIO_ENDPOINT = "http://minio:9000"


def create_spark_session():
    """Create and configure a Spark session for streaming."""
    logger.info("Initializing Spark session with S3 configuration for streaming...")
    
    spark = (SparkSession.builder
        .appName("StockMarketStreamingProcessor")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate())
    
    # Force shuffle partitions setting
    spark.conf.set("spark.sql.shuffle.partitions", 2)
    
    # Configure S3A filesystem
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    # Set Spark log level
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized successfully!")
    
    return spark

def define_schema():
    """Define schema for the stock data."""
    logger.info("Defining schema for stock data...")
    return StructType([
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("change", DoubleType(), True),
        StructField("change_percent", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

def log_raw_data(df, batch_id):
    """Log raw data being processed in each micro-batch."""
    if df.count() > 0:
        logger.info(f"Processing raw data batch {batch_id} with {df.count()} rows")
        # Collect the data and log each row
        for row in df.collect():
            logger.info(f"Raw data row: {row.asDict()}")
    else:
        logger.info(f"Processing raw data batch {batch_id} with 0 rows")

def process_and_write_batch(df, batch_id):
    if df.count() > 0:
        # Log the processed data
        logger.info(f"Processing processed data batch {batch_id} with {df.count()} rows")
        for row in df.collect():
            logger.info(f"Processed data row: {row.asDict()}")

        # Write the batch to S3/MinIO
        output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/"
        logger.info(f"Writing batch {batch_id} to {output_path}")
        df.write \
            .mode("append") \
            .partitionBy("symbol") \
            .parquet(output_path)
    else:
        logger.info(f"Processing processed data batch {batch_id} with 0 rows")

def read_stream_from_s3(spark):
    """
    Read streaming data from S3/MinIO.
    
    Args:
        spark: SparkSession
        
    Returns:
        Streaming DataFrame containing stock data
    """
    logger.info("\n--- Setting Up Streaming Read from S3 ---")
    
    # Define the schema
    schema = define_schema()
    
    # Path where real-time data is stored
    s3_path = f"s3a://{MINIO_BUCKET}/raw/realtime/"
    logger.info(f"Reading streaming data from: {s3_path}")
    
    try:
        # Read streaming data from S3
        streaming_df = (spark.readStream
            .schema(schema)
            .option("header", "true")
            .csv(s3_path))
        
        # Clean and transform the data
        streaming_df = (streaming_df
            .withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("price", F.col("price").cast(DoubleType()))
            .withColumn("change", F.col("change").cast(DoubleType()))
            .withColumn("change_percent", F.regexp_replace("change_percent", "%", ""))
            .withColumn("change_percent", F.col("change_percent").cast(DoubleType()))
            .withColumn("volume", F.col("volume").cast(IntegerType())))
        
        logger.info("Streaming DataFrame schema:")
        logger.info("\n" + streaming_df._jdf.schema().treeString())
        
        # Log the raw data using foreachBatch
        streaming_df.writeStream \
            .foreachBatch(log_raw_data) \
            .outputMode("append") \
            .start()
        
        return streaming_df
    except Exception as e:
        logger.error(f"Error setting up streaming read from {s3_path}: {e}")
        logger.error(traceback.format_exc())
        return None

def process_streaming_data(streaming_df):
    """
    Process streaming stock data to calculate real-time metrics.
    
    Args:
        streaming_df: Streaming DataFrame containing stock data
        
    Returns:
        Processed Streaming DataFrame with additional calculated columns
    """
    logger.info("\n--- Processing Streaming Stock Data ---")
    
    if streaming_df is None:
        logger.info("No streaming data to process")
        return None
    
    try:
        # Add watermark to handle late data
        streaming_df = streaming_df.withWatermark("timestamp", "5 minutes")
        
        # Define sliding windows for real-time metrics
        window_15min = F.window("timestamp", "15 minutes", "5 minutes")  # 15-minute window, sliding every 5 minutes
        window_1h = F.window("timestamp", "1 hour", "10 minutes")  # 1-hour window, sliding every 10 minutes
        
        # Step 1: Compute metrics for the 15-minute window
        df_15min = (streaming_df
            .groupBy(
                F.col("symbol"),
                window_15min.alias("window")
            )
            .agg(
                F.avg("price").alias("ma_15m"),
                F.stddev("price").alias("volatility_15m"),
                F.sum("volume").alias("volume_sum_15m")
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window"))
        
        # Step 2: Compute metrics for the 1-hour window
        df_1h = (streaming_df
            .groupBy(
                F.col("symbol"),
                window_1h.alias("window")
            )
            .agg(
                F.avg("price").alias("ma_1h"),
                F.stddev("price").alias("volatility_1h"),
                F.sum("volume").alias("volume_sum_1h")
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window"))
        
        # Step 3: Join the two DataFrames on symbol and window_start
        processed_df = (df_15min
            .join(
                df_1h,
                (df_15min.symbol == df_1h.symbol) & 
                (df_15min.window_start == df_1h.window_start),
                "inner"
            )
            .select(
                df_15min.symbol,
                df_15min.window_start.alias("window_start"),
                df_15min.window_end.alias("window_15m_end"),
                df_1h.window_end.alias("window_1h_end"),
                df_15min.ma_15m,
                df_1h.ma_1h,
                df_15min.volatility_15m,
                df_1h.volatility_1h,
                df_15min.volume_sum_15m,
                df_1h.volume_sum_1h
            ))
        
        logger.info("Processed Streaming DataFrame schema:")
        logger.info("\n" + processed_df._jdf.schema().treeString())
        
        return processed_df
    except Exception as e:
        logger.error(f"Error processing streaming stock data: {e}")
        logger.error(traceback.format_exc())
        return None

def write_stream_to_s3(processed_df):
    logger.info("\n----- Writing Processed Streaming Data to S3")

    if processed_df is None:
        logger.error("No processed DataFrame to write to S3")
        return None 
    
    output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/"
    logger.info(f"Writing processed streaming data to: {output_path}")

    try:
        checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/streaming_processor"

        query = (processed_df.writeStream
                 .foreachBatch(process_and_write_batch)
                 .trigger(processingTime='1 minute')
                 .option("checkpointLocation", checkpoint_path)
                 .outputMode("append")
                 .start())
        
        logger.info(f"Streaming query started, writing to {output_path}")
        return query
    except Exception as e:
        logger.error(f"Error writing streaming data to S3: {e}")
        logger.error(traceback.format_exc())
        return None

def main():
    """Main function to process real-time stock data using Spark Streaming."""
    logger.info("\n=========================================")
    logger.info("STARTING STOCK MARKET STREAMING PROCESSOR")
    logger.info("=========================================\n")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read streaming data from S3
        streaming_df = read_stream_from_s3(spark)
        
        if streaming_df is not None:
            # Process streaming data
            processed_df = process_streaming_data(streaming_df)
            
            if processed_df is not None:
                # Write processed data to S3
                query = write_stream_to_s3(processed_df)
                
                if query is not None:
                    logger.info("\nStreaming processor is running...")
                    query.awaitTermination()  # Wait for the streaming to finish (runs indefinitely)
                else:
                    logger.info("\nFailed to start streaming query")
            else:
                logger.info("\nNo processed streaming data to write")
        else:
            logger.info("\nNo streaming data to process")
            
    except Exception as e:
        logger.error(f"\nError in streaming processing: {e}")
        logger.error(traceback.format_exc())
    finally:
        # Stop Spark session
        logger.info("\nStopping Spark session")
        spark.stop()
        logger.info("\n=========================================")
        logger.info("STREAMING PROCESSING COMPLETED")
        logger.info("=========================================")

if __name__ == "__main__":
    main()