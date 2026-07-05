from common import get_spark_session, twitch_api_schema, TOPIC_NAME
from pyspark.sql.functions import col, from_json, explode, year, month, day

# Initialize Spark session
spark = get_spark_session("TwitchNoNameStreamingConsumerBronze", master="spark://spark-master:7077")

# Flow from Kafka to Spark
kafka_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("subscribe", TOPIC_NAME) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load()

# Parse JSON and explode the stream data array as rows into the main table
exploded_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload", "timestamp as ingestion_ts") \
  .withColumn("parsed_json", from_json(col("json_payload"), twitch_api_schema)) \
  .withColumn("stream", explode(col("parsed_json.data")))

# Select all the stream columns and the ingestion timestamp
bronze_df = exploded_df.select(
  col("ingestion_ts"),
  col("stream.id").alias("stream_id"),
  col("stream.user_name"),
  col("stream.game_name"), # can be an empty string
  col("stream.title"), # can be an empty string
  col("stream.tags"),
  col("stream.viewer_count"),
  col("stream.started_at"),
  col("stream.language"), # ISO 639-1 language code or "other"
  col("stream.thumbnail_url")
)

# Extraction of ingestion year, month and day columns to partition later
bronze_time_df = bronze_df \
  .withColumn("year", year(col("ingestion_ts"))) \
  .withColumn("month", month(col("ingestion_ts"))) \
  .withColumn("day", day(col("ingestion_ts")))

# Write stream in Delta Lake format in bucket 'twitch-bronze'
query = bronze_time_df.writeStream \
  .outputMode("append") \
  .format("delta") \
  .partitionBy("year", "month", "day") \
  .option("checkpointLocation", "s3a://twitch-bronze/checkpoints/streams/") \
  .start("s3a://twitch-bronze/streams/")

query.awaitTermination()