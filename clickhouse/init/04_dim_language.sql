CREATE TABLE IF NOT EXISTS twitch.dim_language
(
  language_id Int64,
  language String,

  inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY language_id;