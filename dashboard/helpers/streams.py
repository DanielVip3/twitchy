from datetime import datetime, timezone
import polars as pl

STREAM_TIMEOUT_HOURS = 12  # streams are online / live only if new data has been received within 12 hours

def latest_snapshot(df: pl.DataFrame, at_time: datetime | None = None) -> pl.DataFrame:
  """
  Returns the latest snapshot of the API, allowing for time-traveling (i.e. latest at a given point in time).
  """
  
  if at_time is None: # Default behavior: most recent snapshot
    return df.filter(pl.col("ingestion_ts") == pl.col("ingestion_ts").max())

  else: # Most recent snapshot per stream at or before the selected time
    if at_time.tzinfo is None:
      at_time = at_time.replace(tzinfo=timezone.utc)

    filtered = df.filter(pl.col("ingestion_ts") <= pl.lit(at_time))

    if filtered.is_empty(): # If the time is too early, fallback to earliest available
      return df.filter(pl.col("ingestion_ts") == pl.col("ingestion_ts").min())

    return filtered.filter(pl.col("ingestion_ts") == pl.col("ingestion_ts").max())


def top_games(df: pl.DataFrame, n: int) -> pl.DataFrame:
  """
  Returns the top n games by viewer count.
  """

  return df \
    .group_by("game_name") \
    .agg(pl.col("viewer_count").sum().alias("total_viewers"), pl.len().alias("streams")) \
    .sort("total_viewers", descending=True) \
    .head(n)


def top_streamers(streams: pl.DataFrame, latest: pl.DataFrame, n: int) -> pl.DataFrame:
  """
  Returns the top n online streamers by recent average viewer count.
  """

  # Only consider streams that are currently online
  online_stream_ids = latest.select("stream_id").unique()

  # Compute rolling mean per stream_id for those online streams
  online_streams = streams \
    .filter(pl.col("stream_id").is_in(online_stream_ids["stream_id"])) \
    .sort("ingestion_ts") \
    .with_columns(
      pl.col("viewer_count").rolling_mean(window_size=10, min_periods=1).over("stream_id").round(decimals=0).cast(pl.Int32).alias("avg_viewers_10")
    )

  # Aggregate by user_name and sort by rolling mean
  return online_streams \
    .sort("viewer_count", descending=True) \
    .group_by("user_name") \
    .agg(
      pl.col("viewer_count").first().alias("latest_viewer_count"),
      pl.col("viewer_count").max().alias("peak_viewers"),
      pl.col("avg_viewers_10").first().alias("avg_viewers_10")
    ) \
    .sort("avg_viewers_10", descending=True) \
    .head(n)


def top_tags_by_frequency(tags: pl.DataFrame, n: int) -> pl.DataFrame:
  """
  Returns the top n tags by usage.
  """

  return tags \
    .group_by("tag_name") \
    .agg(pl.len().alias("uses")) \
    .sort("uses", descending=True) \
    .head(n)


def top_tags_by_viewers(tags: pl.DataFrame, streams: pl.DataFrame, n: int) -> pl.DataFrame:
  """
  Returns the top n tags by viewer count.
  """

  streams_df = streams.select("stream_id", "ingestion_ts", "viewer_count")

  # Join each tag usage to the viewer count of that exact snapshot.
  # Then average viewers per tag, and drop unique tags (very few uses).
  return tags \
    .join(streams_df, on=["stream_id", "ingestion_ts"]) \
    .group_by("tag_name") \
    .agg(pl.col("viewer_count").mean().alias("avg_viewers"), pl.len().alias("uses")) \
    .filter(pl.col("uses") >= 5) \
    .sort("avg_viewers", descending=True) \
    .head(n)


def format_datetime(dt: datetime) -> str:
  return dt.strftime("%B %d, %Y - %H:%M:%S")