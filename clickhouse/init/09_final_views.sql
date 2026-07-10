CREATE VIEW IF NOT EXISTS dim_game_final AS
SELECT *
FROM dim_game
FINAL;

CREATE VIEW IF NOT EXISTS dim_date_final AS
SELECT *
FROM dim_date
FINAL;

CREATE VIEW IF NOT EXISTS dim_streamer_final AS
SELECT *
FROM dim_streamer
FINAL;

CREATE VIEW IF NOT EXISTS dim_language_final AS
SELECT *
FROM dim_language
FINAL;

CREATE VIEW IF NOT EXISTS fact_stream_hourly_final AS
SELECT *
FROM fact_stream_hourly
FINAL;