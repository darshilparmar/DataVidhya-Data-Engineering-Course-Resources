from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, to_date, monotonically_increasing_id

spark = SparkSession.builder.appName("TaxiETL").getOrCreate()

# Input / Output paths
input_path = "s3://aws-glue-assets/input/run-*"
dim_output = "s3://aws-glue-assets/output/processed/dimensions/"
fact_output = "s3://aws-glue-assets/output/processed/facts/"

#  Read CSV
df = spark.read.csv(input_path, header=True, inferSchema=True)

# --- Step 1: Dimensions with surrogate keys ---

# Vendor Dimension
dim_vendor = (
    df.select("vendor_id").distinct()
      .withColumn("vendor_key", monotonically_increasing_id())
)
dim_vendor.write.mode("overwrite").parquet(dim_output + "dim_vendor/")

# Location Dimension
dim_location = (
    df.select("pickup_borough", "pickup_location", "dropoff_borough", "dropoff_location")
      .distinct()
      .withColumn("location_key", monotonically_increasing_id())
)
dim_location.write.mode("overwrite").parquet(dim_output + "dim_location/")

# Payment Dimension
dim_payment = (
    df.select("payment_type").distinct()
      .withColumn("payment_key", monotonically_increasing_id())
)
dim_payment.write.mode("overwrite").parquet(dim_output + "dim_payment/")

# --- Step 2: Fact Table ---
df = df.withColumn("trip_date", to_date(col("pickup_datetime")))

# Join with dimensions to replace natural keys → surrogate keys
fact_trips = (
    df.join(dim_vendor, "vendor_id", "left")
      .join(dim_payment, "payment_type", "left")
      .join(dim_location,
            (df.pickup_borough == dim_location.pickup_borough) &
            (df.pickup_location == dim_location.pickup_location) &
            (df.dropoff_borough == dim_location.dropoff_borough) &
            (df.dropoff_location == dim_location.dropoff_location),
            "left"
      )
      .groupBy("vendor_key", "payment_key", "location_key", "trip_date")
      .agg(
          count("*").alias("total_trips"),
          _sum("total_amount").alias("total_revenue"),
          avg("trip_distance").alias("avg_distance"),
          avg("fare_amount").alias("avg_fare"),
          _sum("tip_amount").alias("total_tips")
      )
)

fact_trips.write.mode("overwrite").parquet(fact_output + "fact_trips/")

print(" Dimensions and facts with surrogate keys written to S3")
