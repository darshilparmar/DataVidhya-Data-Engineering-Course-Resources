from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, to_date, monotonically_increasing_id

spark = SparkSession.builder.appName("TaxiETL").getOrCreate()

# Input / Output paths
input_path = "s3://aws-glue-assets-640958509818-us-east-1/input/run-*"
dim_output = "s3://aws-glue-assets-640958509818-us-east-1/output/processed/dimensions/"
fact_output = "s3://aws-glue-assets-640958509818-us-east-1/output/processed/facts/"

# Read CSV with proper options for your data format
df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .option("quote", "\"") \
               .option("escape", "\"") \
               .csv(input_path)=

# Debug: Check the schema and data
print("=== Schema ===")
df.printSchema()
print("=== Sample Data ===")
df.show(5, truncate=False)
print("=== Column Names ===")
print(df.columns)

# --- Step 1: Dimensions with surrogate keys ---

# Vendor Dimension
print("Creating Vendor Dimension...")
dim_vendor = (
    df.select("vendor_id", "vendor_name").distinct()
      .withColumn("vendor_key", monotonically_increasing_id())
)
print("Vendor Dimension:")
dim_vendor.show()
dim_vendor.write.mode("overwrite").parquet(dim_output + "dim_vendor/")

# Location Dimension
print("Creating Location Dimension...")
dim_location = (
    df.select("pickup_city", "pickup_location", "dropoff_city", "dropoff_location")
      .distinct()
      .withColumn("location_key", monotonically_increasing_id())
)
print("Location Dimension:")
dim_location.show()
dim_location.write.mode("overwrite").parquet(dim_output + "dim_location/")

# Payment Dimension
print("Creating Payment Dimension...")
dim_payment = (
    df.select("payment_type").distinct()
      .withColumn("payment_key", monotonically_increasing_id())
)
print("Payment Dimension:")
dim_payment.show()
dim_payment.write.mode("overwrite").parquet(dim_output + "dim_payment/")

# --- Step 2: Fact Table ---
print("Creating Fact Table...")
df = df.withColumn("trip_date", to_date(col("pickup_datetime")))

# Join with dimensions to replace natural keys → surrogate keys
fact_trips = (
    df.alias("fact")
      .join(dim_vendor.alias("vendor"), "vendor_id", "left")
      .join(dim_payment.alias("payment"), "payment_type", "left")
      .join(dim_location.alias("loc"),
            (col("fact.pickup_city") == col("loc.pickup_city")) &
            (col("fact.pickup_location") == col("loc.pickup_location")) &
            (col("fact.dropoff_city") == col("loc.dropoff_city")) &
            (col("fact.dropoff_location") == col("loc.dropoff_location")),
            "left"
      )
      .groupBy("vendor_key", "payment_key", "location_key", "trip_date")
      .agg(
          count("*").alias("total_trips"),
          _sum("total_amount").alias("total_revenue"),
          avg("trip_distance").alias("avg_distance"),
          avg("fare_amount").alias("avg_fare"),
          _sum(col("tolls_amount")).alias("total_tolls")  # Fixed column name
      )
)

print("Fact Table:")
fact_trips.show()
fact_trips.write.mode("overwrite").parquet(fact_output + "fact_trips/")

print("✅ Dimensions and facts with surrogate keys written to S3")
print(f"Vendor dimension count: {dim_vendor.count()}")
print(f"Location dimension count: {dim_location.count()}")
print(f"Payment dimension count: {dim_payment.count()}")
print(f"Fact table count: {fact_trips.count()}")

spark.stop()