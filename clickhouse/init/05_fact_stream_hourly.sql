CREATE TABLE IF NOT EXISTS twitch.fact_stream_hourly
(
  /* Dimensions */
  date_id UInt32,
  time_hour UInt8, /* degenerate time dimension */
  game_id Int64,
  streamer_id Int64,
  language_id Int64,

  /* Measures */
  max_viewers UInt32,
  sum_viewers Float64,
  count_observations UInt32,

  inserted_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree(inserted_at)
ORDER BY (date_id, game_id, time_hour, language_id, streamer_id);