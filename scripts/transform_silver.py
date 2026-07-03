from common import get_spark_session, gbfs_schema
from pyspark.sql.functions import col, from_json, explode, from_unixtime, to_timestamp, year, month, day

# Initialize Spark session
spark = get_spark_session("DotTurinTransformBronzeToSilver")

print("[*] Reading raw data from bucket, parsing JSON and transforming...")

bronze_stream_df = spark.readStream \
  .format("delta") \
  .load("s3a://dotturin-raw/bikes/")

parsed_df = bronze_stream_df.withColumn("parsed_json", from_json(col("json_payload"), gbfs_schema))

# Explode the bike array as rows into the main table
exploded_df = parsed_df.withColumn("bike", explode(col("parsed_json.data.bikes")))

# Select all the bikes columns, the received timestamp and the last updated timestamp
silver_df = exploded_df.select(
  col("timestamp"), # this is already a timestamp in microseconds, unlike the others which are in seconds
  to_timestamp(from_unixtime(col("parsed_json.last_updated"))).alias("last_updated"),
  col("bike.*")
) \
.withColumn("last_reported", to_timestamp(from_unixtime(col("last_reported"))))

silver_df.printSchema()

# Deduplicate data, as bronze ingestion may include duplicates.
# Since we are in a streaming process, the last_updated timestamp must be saved across triggers
# to check for duplicates. Using withWatermark we specify that it is necessary to store only
# timestamps only for 2 hours, because we assume that after that time at least one request
# will produce fresh data. It is resilient in case of Dott API errors, but only with duplicates
# with less than 2 hours of distance.
silver_deduplicated_df = silver_df \
  .withWatermark("last_updated", "2 hours") \
  .dropDuplicates(["bike_id", "last_updated"])

# Extraction of last updated year, month and day columns to partition later
silver_time_df = silver_deduplicated_df \
  .withColumn("year", year(col("last_updated"))) \
  .withColumn("month", month(col("last_updated"))) \
  .withColumn("day", day(col("last_updated")))

print("[*] Writing transformed data in bucket...")

# Write incrementally, partitioned by year, month and day, the transformed data stream in silver bucket.
# trigger(availableNow=True) reads all unread data from the last trigger.
query = silver_time_df.writeStream \
  .outputMode("append") \
  .format("delta") \
  .partitionBy("year", "month", "day") \
  .option("checkpointLocation", "s3a://dotturin-processed/checkpoints/silver_bikes/") \
  .trigger(availableNow=True) \
  .start("s3a://dotturin-processed/bikes_status/")

query.awaitTermination()

print("[+] Transformation completed successfully.")