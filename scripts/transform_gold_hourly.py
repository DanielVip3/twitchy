from common import get_spark_session
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, sum, count, expr, concat, lpad

# Initialize Spark session
spark = get_spark_session("TwitchNoNameGoldHourly")

CH_HOST = "clickhouse"
CH_PORT = "8123" 
CH_USER = os.environ.get("CLICKHOUSE_USER")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CH_DB = os.environ.get("CLICKHOUSE_DB")

# Read data from silver layer streams Delta Lake
silver_streams_df = spark.readStream \
  .format("delta") \
  .load("s3a://twitch-silver/streams")

# Read games as a static DF to join later
silver_games_df = spark.read \
  .format("delta") \
  .load("s3a://twitch-silver/games")

def write_to_clickhouse(df: DataFrame, table: str):
  df.write \
    .format("clickhouse") \
    .option("host", CH_HOST) \
    .option("protocol", "http") \
    .option("http_port", CH_PORT) \
    .option("database", CH_DB) \
    .option("table", table) \
    .option("user", CH_USER) \
    .option("password", CH_PASSWORD) \
    .mode("append") \
    .save()

def process_gold(batch_df: DataFrame, _: int):
  """
  Processes the hourly micro-batch to insert into Dimensions and Fact tables.
  """

  # Clean and fill missing dimension keys to avoid breaking facts
  clean_batch_df = batch_df.filter(
    col("game_name").isNotNull() & (expr("trim(game_name)") != "")
  ).fillna({
    "user_name": "Unknown",
    "language": "Unknown"
  })

  clean_batch_df.persist()


  # Streamer dimension table
  dim_streamer_df = clean_batch_df \
    .select("user_name") \
    .distinct() \
    .withColumn("streamer_id", expr("xxhash64(user_name)"))

  write_to_clickhouse(dim_streamer_df, "dim_streamer")


  # Language dimension table
  dim_language_df = clean_batch_df \
    .select("language") \
    .distinct() \
    .withColumn("language_id", expr("xxhash64(language)"))

  write_to_clickhouse(dim_language_df, "dim_language")


  # Game dimension table
  # Left outer join with games to include some IGDB data if available
  dim_game_df = clean_batch_df \
    .select("game_name") \
    .distinct() \
    .withColumn("game_id", expr("xxhash64(game_name)")) \
    .join(silver_games_df.drop("game_id"), on="game_name", how="left") \
    .select(
      "game_id",
      "game_name",
      "igdb_id",
      "total_rating",
      "total_rating_count",
      "first_release_date",
      "themes",
      "keywords",
      "platforms"
    )

  write_to_clickhouse(dim_game_df, "dim_game")


  # Date dimension table
  dim_date_df = batch_df \
    .select(
      col("started_year").alias("date_year"),
      col("started_month").alias("date_month"),
      col("started_day").alias("date_day")
    ) \
    .distinct() \
    .withColumn("date_id", # number like 20260707
      concat(
        col("date_year"), 
        lpad(col("date_month"), 2, "0"), 
        lpad(col("date_day"), 2, "0")
      ).cast("long")
    )

  write_to_clickhouse(dim_date_df, "dim_date")


  # fact_stream_hourly Fact table
  # Dimensions: game, streamer, language, date, time
  # Measures: max viewers, avg viewers and number of observations in API
  fact_stream_hourly_df = clean_batch_df \
    .withColumn("game_id", expr("xxhash64(game_name)")) \
    .withColumn("streamer_id", expr("xxhash64(user_name)")) \
    .withColumn("language_id", expr("xxhash64(language)")) \
    .withColumn("date_id",
      concat(
        col("started_year"),
        lpad(col("started_month"), 2, "0"),
        lpad(col("started_day"), 2, "0")
      ).cast("long")
    ) \
    .groupBy(
      "game_id",
      "streamer_id",
      "language_id",
      "date_id",
      col("started_hour").alias("time_hour")
    ).agg(
      max("viewer_count").alias("max_viewers"),
      sum("viewer_count").alias("sum_viewers"),
      count("viewer_count").alias("count_observations")
    )

  write_to_clickhouse(fact_stream_hourly_df, "fact_stream_hourly")

  batch_df.unpersist()

# Write incrementally (i.e. batch-like)
query = silver_streams_df.writeStream \
  .outputMode("update") \
  .foreachBatch(process_gold) \
  .option("checkpointLocation", "s3a://twitch-gold/checkpoints/fact_game_hourly/") \
  .trigger(availableNow=True) \
  .start()

query.awaitTermination()
spark.stop()