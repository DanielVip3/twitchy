CREATE TABLE IF NOT EXISTS twitch.dim_game
(
  game_id Int64,
  game_name String,

  igdb_id Nullable(String),
  total_rating Nullable(Float64),
  total_rating_count Nullable(Int64),
  first_release_date Nullable(DateTime),
  themes Array(String),     
  keywords Array(String),   
  platforms Array(String),  
  
  inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY game_id;