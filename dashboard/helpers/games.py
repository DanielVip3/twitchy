import polars as pl

def latest_games_snapshot(games: pl.DataFrame) -> pl.DataFrame:
  """
  Deduplicates the games table, keeping only the most recent ingestion per game_id, as IGDB metadata can change.
  """

  if games.is_empty():
    return games

  return games \
    .sort("ingestion_ts", descending=True) \
    .unique(subset=["game_id"], keep="first")


def top_rated_games(games: pl.DataFrame, n: int, min_rating_count: int = 5) -> pl.DataFrame:
  """
  Top n games by IGDB rating, ignoring games with too few ratings to be meaningful.
  """

  return games \
    .filter(pl.col("total_rating").is_not_null() & (pl.col("total_rating_count") >= min_rating_count)) \
    .sort("total_rating", descending=True) \
    .select("game_name", "total_rating", "total_rating_count", "url") \
    .head(n)


def _category_frequency(games: pl.DataFrame, list_col: str, alias: str, n: int) -> pl.DataFrame:
  """
  Return the n most frequent categories, i.e. values, of a given list_col, that gets renamed to an alias.
  """

  exploded = games \
    .select("game_id", list_col) \
    .filter(pl.col(list_col).is_not_null()) \
    .explode(list_col) \
    .rename({list_col: alias}) \
    .filter(pl.col(alias).is_not_null() & (pl.col(alias) != ""))

  if exploded.is_empty():
    return pl.DataFrame({alias: [], "count": []})

  return exploded \
    .group_by(alias) \
    .agg(pl.len().alias("count")) \
    .sort("count", descending=True) \
    .head(n)


def theme_frequency(games: pl.DataFrame, n: int) -> pl.DataFrame:
  return _category_frequency(games, "themes", "theme", n)


def game_mode_frequency(games: pl.DataFrame, n: int) -> pl.DataFrame:
  return _category_frequency(games, "game_modes", "game_mode", n)


def player_perspective_frequency(games: pl.DataFrame, n: int) -> pl.DataFrame:
  return _category_frequency(games, "player_perspectives", "player_perspective", n)


def platform_type_frequency(games: pl.DataFrame, n: int) -> pl.DataFrame:
  return _category_frequency(games, "platform_types", "platform_type", n)


def platform_family_frequency(games: pl.DataFrame, n: int) -> pl.DataFrame:
  return _category_frequency(games, "platform_families", "platform_family", n)


def keyword_frequency(games: pl.DataFrame, n: int) -> pl.DataFrame:
  return _category_frequency(games, "keywords", "keyword", n)


def release_year_trend(games: pl.DataFrame) -> pl.DataFrame:
  """
  Returns number of tracked games and average rating per release year.
  """

  dated = games.filter(pl.col("first_release_date").is_not_null())

  if dated.is_empty():
    return dated

  return dated \
    .with_columns(pl.col("first_release_date").dt.year().alias("release_year")) \
    .group_by("release_year") \
    .agg(
      pl.len().alias("games_released"),
      pl.col("total_rating").mean().alias("avg_rating"),
    ) \
    .sort("release_year")


def rating_vs_popularity(games: pl.DataFrame, latest_streams: pl.DataFrame) -> pl.DataFrame:
  """
  Return rating and current Twitch viewership per game name, to compare critical acclaim vs. live streaming popularity.
  """

  viewers_by_game = latest_streams \
    .group_by("game_name") \
    .agg(
      pl.col("viewer_count").sum().alias("current_viewers"),
      pl.len().alias("live_streams"),
    )

  return games \
    .filter(pl.col("total_rating").is_not_null()) \
    .join(viewers_by_game, on="game_name", how="inner") \
    .select("game_name", "total_rating", "total_rating_count", "current_viewers", "live_streams")


def rating_popularity_quadrants(merged: pl.DataFrame) -> pl.DataFrame:
  """
  Splits games into 4 quadrants around the median rating and median current viewer count: "Hidden gem" (high rating, low viewership), "Mainstream"
  (high rating, high viewership), "Overhyped" (low rating, high viewership) and "Niche" (low rating, low viewership).
  Medians are computed from the merged data itself.
  """

  if merged.is_empty():
    return merged.with_columns(pl.lit(None).alias("quadrant"))

  rating_median = merged["total_rating"].median()
  viewers_median = merged["current_viewers"].median()

  return merged.with_columns(
    pl.when((pl.col("total_rating") >= rating_median) & (pl.col("current_viewers") < viewers_median))
      .then(pl.lit("Hidden gem"))
    .when((pl.col("total_rating") >= rating_median) & (pl.col("current_viewers") >= viewers_median))
      .then(pl.lit("Mainstream hit"))
    .when((pl.col("total_rating") < rating_median) & (pl.col("current_viewers") >= viewers_median))
      .then(pl.lit("Overhyped"))
    .otherwise(pl.lit("Niche"))
    .alias("quadrant")
  )