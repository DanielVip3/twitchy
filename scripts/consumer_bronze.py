from common import get_spark_session, stream_schema, game_schema, TOPIC_STREAMS, TOPIC_GAMES
from pyspark.sql.functions import col, from_json, explode, year, month, day

# Initialize Spark session
spark = get_spark_session("TwitchNoNameStreamingConsumerBronze", master="spark://spark-master:7077")

# -- STREAMS TOPIC
# Flow from Kafka to Spark
kafka_streams_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("subscribe", TOPIC_STREAMS) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load()

# Parse JSON and explode the stream data array as rows into the main table
exploded_streams_df = kafka_streams_df.selectExpr("CAST(value AS STRING) as json_payload", "timestamp as ingestion_ts") \
  .withColumn("parsed_json", from_json(col("json_payload"), stream_schema)) \
  .withColumn("stream", explode(col("parsed_json.data")))

# Select all the stream columns and the ingestion timestamp
bronze_streams_df = exploded_streams_df.select(
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
bronze_streams_time_df = bronze_streams_df \
  .withColumn("year", year(col("ingestion_ts"))) \
  .withColumn("month", month(col("ingestion_ts"))) \
  .withColumn("day", day(col("ingestion_ts")))

# Write stream in Delta Lake format in bucket 'twitch-bronze'
query = bronze_streams_time_df.writeStream \
  .outputMode("append") \
  .format("delta") \
  .partitionBy("year", "month", "day") \
  .option("checkpointLocation", "s3a://twitch-bronze/checkpoints/streams/") \
  .start("s3a://twitch-bronze/streams/")



# -- GAMES TOPIC
kafka_games_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("subscribe", TOPIC_GAMES) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load()

# Parse JSON and explode
exploded_games_df = kafka_games_df.selectExpr("CAST(value AS STRING) as json_payload", "timestamp as ingestion_ts") \
  .withColumn("parsed_json", from_json(col("json_payload"), game_schema)) \
  .withColumn("game", explode(col("parsed_json.data")))

# Select flattened game columns and nested IGDB columns
bronze_games_df = exploded_games_df.select(
  col("ingestion_ts"),
  col("game.id").alias("game_id"),
  col("game.name").alias("game_name"),
  col("game.igdb_id"),
  col("game.igdb_data.summary"),
  col("game.igdb_data.total_rating"),
  col("game.igdb_data.total_rating_count"),
  col("game.igdb_data.first_release_date"),
  col("game.igdb_data.storyline"),
  col("game.igdb_data.url"),
  col("game.igdb_data.themes"),
  col("game.igdb_data.player_perspectives"),
  col("game.igdb_data.platforms"),
  col("game.igdb_data.platform_families"),
  col("game.igdb_data.platform_types"),
  col("game.igdb_data.keywords"),
  col("game.igdb_data.game_modes")
)

# Write stream in Delta Lake format in bucket 'twitch-bronze' (not partitioned)
query = bronze_games_df.writeStream \
  .outputMode("append") \
  .format("delta") \
  .option("checkpointLocation", "s3a://twitch-bronze/checkpoints/games/") \
  .start("s3a://twitch-bronze/games/")


spark.streams.awaitAnyTermination()