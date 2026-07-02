import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, sum, when

load_dotenv()

# Initialize Spark session
spark = SparkSession.builder \
  .appName("DotTurinTransformSilverToGold") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
  .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
  .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
  .config("spark.hadoop.fs.s3a.path.style.access", "true") \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") \
  .config("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true") \
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("[*] Reading data...")

silver_df = spark.read \
  .format("delta") \
  .load("s3a://dotturin-processed/bikes_status/")

# Add hour column from last_updated timestamp
silver_with_hour_df = silver_df \
  .withColumn("hour", hour(col("last_updated")))

print("[*] Aggregating fleet by date and hour...")

# Group by date and hour, computing the amount of bikes (total, split in reserved, disabled and available)
gold_hourly_fleet_df = silver_with_hour_df \
  .groupBy("year", "month", "day", "hour") \
  .agg(
    count("bike_id").alias("total_bikes"),
    sum(when(col("is_reserved") == True, 1).otherwise(0)).alias("reserved_bikes"),
    sum(when(col("is_disabled") == True, 1).otherwise(0)).alias("disabled_bikes"),
    sum(when((col("is_reserved") == False) & (col("is_disabled") == False), 1).otherwise(0)).alias("available_bikes")
  )

print("[*] Writing data...")

# Write the hourly fleet status partitioned by month
# mode("overwrite") as we every time recompute all from scratch every time
gold_hourly_fleet_df.write \
  .format("delta") \
  .mode("overwrite") \
  .partitionBy("year", "month") \
  .save("s3a://dotturin-processed/hourly_fleet_status/")

print("[+] Transformation completed successfully.")