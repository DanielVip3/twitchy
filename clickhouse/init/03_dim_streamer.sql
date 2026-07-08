CREATE TABLE IF NOT EXISTS twitch.dim_streamer
(
  streamer_id Int64,
  user_name String,

  inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY streamer_id;