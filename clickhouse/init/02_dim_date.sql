CREATE TABLE IF NOT EXISTS twitch.dim_date
(
  date_id UInt32,

  date_year UInt16,
  date_month UInt8,
  date_day UInt8,

  inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PRIMARY KEY date_id
ORDER BY (date_id, date_year, date_month, date_day);