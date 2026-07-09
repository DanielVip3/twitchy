# Data dictionary

This document covers the three layers of the data pipeline: bronze (raw, Delta Lake on MinIO), silver (enriched, Delta Lake on MinIO) and gold (star schema, ClickHouse).

Bucket layout on MinIO:

```
twitch-bronze/
  streams/              <- (Delta table, partitioned by Y, M and D)
  games/                <- (Delta table, not partitioned)
  checkpoints/...
twitch-silver/
  streams/              <- (Delta table, not partitioned)
  stream_tags/          <- (Delta table, not partitioned)
  stream_transitions/   <- (Delta table, not partitioned)
  games/                <- (Delta table, not partitioned)
  checkpoints/...
twitch-gold/
  checkpoints/...
```

The gold data lives in ClickHouse data warehouse, while the Spark Structured Streaming checkpoints live in `twitch-gold/checkpoints` on MinIO.

---

**NOTE:** One thing worth keeping in mind everywhere below: `ingestion_ts` is the Kafka message timestamp, i.e. the moment the producer polled the Twitch API, not when Spark got around to processing it. It's the closest thing this pipeline has to a "snapshot time", since the source data is a full poll of the top 100 live streams every minute, not an event stream in the traditional sense.

---

## Bronze layer (Delta Lake)

Raw Twitch API data straight out of Kafka, JSON-parsed and exploded into rows. Nothing is dropped here on purpose, as bronze should be replayable from scratch if a silver job needs to be rebuilt. Some fields are reshaped to simplify API usage.

### `twitch-bronze/streams`

One row per live stream per poll. Partitioned by `year`, `month` and `day` (computed from `ingestion_ts`).

Raw data comes from Twitch API `GET https://api.twitch.tv/helix/streams` and is [documented here](https://dev.twitch.tv/docs/api/reference/#get-streams).

| Column | Type | Notes |
|---|---|---|
| `ingestion_ts` | timestamp | Kafka message timestamp, i.e. poll time. |
| `stream_id` | string | Twitch's live stream ID. Changes if the streamer goes offline and comes back (it is **not** a stable streamer identifier). |
| `user_name` | string | Streamer's display name (sufficiently unique). |
| `game_name` | string | Game (or category) currently being "played" in the stream. Can be an empty string if the category isn't set. |
| `title` | string | Stream title, can also be empty. |
| `tags` | array\<string\> | List of live stream tags (set freely by the streamer). |
| `viewer_count` | int | Number of viewers of the live stream at time `ingestion_ts`. |
| `started_at` | timestamp | Timestamp representing when the stream went live. |
| `language` | string | ISO 639-1 code, or the literal string `"other"`, encoding the language of the stream. |
| `thumbnail_url` | string | The URL to the current thumbnail image for the livestream, containing a literal `{width}x{height}` placeholder. Unused in the pipeline currently. |
| `year`, `month`, `day` | int | Partition columns, derived from `ingestion_ts`. |

### `twitch-bronze/games`

One row per unique game seen in a poll, enriched with IGDB metadata at
ingestion time (through a separate API request). Not partitioned as the volume here is an order of magnitude smaller than streams (dozens of unique games per poll vs. up to 100 streams).

Raw data comes from Twitch API `GET https://api.twitch.tv/helix/games` [documented here](https://dev.twitch.tv/docs/api/reference/#get-games), and enriched with IGDB API `POST https://api.igdb.com/v4/games` [documented here](https://api-docs.igdb.com/#game).

| Column | Type | Notes |
|---|---|---|
| `ingestion_ts` | timestamp | Poll time. |
| `raw_payload` | string | The full untouched JSON payload for this batch, kept around in case silver needs a field that wasn't selected (IGDB data contains many unused fields). |
| `game_id` | string | Twitch's game ID. |
| `game_name` | string | The game name (sufficiently unique for the purpose of this pipeline). |
| `igdb_id` | string? | IGDB's game ID. It can be null if Twitch has no IGDB mapping for this game |
| `igdb_data` | struct? | Raw nested IGDB response (see below). |

The POST request to IGDB API queries only specific fields, thus `igdb_data` has this kind of JSON structure (example):
```json
[
  {
    "id": 117883,
    "game_modes": [
        {
            "id": 1,
            "name": "Single player"
        }
    ],
    "keywords": [
        {
            "id": 35,
            "name": "greek mythology"
        },
        {
            "id": 25805,
            "name": "kratos"
        }
    ],
    "platforms": [
        {
            "id": 9,
            "generation": 7,
            "name": "PlayStation 3",
            "platform_family": {
                "id": 1,
                "name": "PlayStation"
            },
            "platform_type": {
                "id": 1,
                "name": "Console"
            }
        },
        {
            "id": 46,
            "generation": 8,
            "name": "PlayStation Vita",
            "platform_family": {
                "id": 1,
                "name": "PlayStation"
            },
            "platform_type": {
                "id": 5,
                "name": "Portable_console"
            }
        }
    ],
    "player_perspectives": [
        {
            "id": 2,
            "name": "Third person"
        }
    ],
    "storyline": "Kratos is a warrior who serves the Greek gods of Olympus. Flashbacks reveal that he was once a successful but bloodthirsty captain in the Spartan army and led his men to several victories before being defeated by a barbarian king. Facing death, Kratos called on the God of War, Ares, whom he promised to serve if the god would spare his men and provide the power to destroy their enemies. Ares agreed and bonded the Blades of Chaos, a pair of chained blades forged in the depths of Tartarus, to his new servant. Kratos, equipped with the blades, then decapitated the barbarian king.",
    "themes": [
        {
            "id": 1,
            "name": "Action"
        },
        {
            "id": 17,
            "name": "Fantasy"
        },
        {
            "id": 22,
            "name": "Historical"
        }
    ],
    "url": "https://www.igdb.com/games/god-of-war--2"
  }
]
```

Additional details on the meaning and values of the fields are available in the [IGDB API documentation](https://api-docs.igdb.com/#game).

---

## Silver layer (Delta Lake)

Bronze data cleaned up, flattened, and split into purpose-specific tables. Tables are generated incrementally and independently from each other.

### `twitch-silver/streams`

Identical to bronze streams but with `tags` dropped (tags live in their own table now) and two derived columns added.  
**Grain:** one row per stream per poll (same as bronze streams).

| Column | Type | Notes |
|---|---|---|
| Everything from bronze streams except `tags`, `year`, `month`, `day` | | |
| `started_year`, `started_month`, `started_day`, `started_hour` | int | Derived from `started_at` (not `ingestion_ts`), useful for grouping by when a stream actually began. |
| `thumbnail_url_1080p` | string | `thumbnail_url` with the `{width}x{height}` placeholder resolved to `1920x1080`. Unused in the pipeline currently. |
| `stream_time_seconds` | int | `ingestion_ts - started_at` in seconds, counting how long the stream has been running at the moment of this snapshot. |

### `twitch-silver/stream_tags`

Tags exploded out of bronze streams into their own table.  
**Grain:** one row per tag per stream per poll.

| Column | Notes |
|---|---|
| `stream_id`, `ingestion_ts` | Foreign keys to streams (silver) table. |
| `tag_name` | Lowercased and trimmed. |

### `twitch-silver/stream_transitions`

Contains events detected through stateful processing (over two consecutive polls), that can either be game (category) changes or live stream title changes. 
**Grain:** one row per event per stream per poll.

| Column | Type | Notes |
|---|---|---|
| `stream_id`, `event_ts` | | Foreign keys to streams (silver) table. `event_ts` corresponds to the `ingestion_ts` of the snapshot where the change was detected. |
| `user_name` | string | Denormalized column to access streamer name (redundant to optimize read performance). |
| `event_type` | string | The type of event, either `GAME_CHANGE` or `TITLE_CHANGE`. |
| `old_value` | string | The old field value (game or title). |
| `new_value` | string | The updated field value (game or title). |
| `viewer_delta` | int | Viewer count change between the two snapshots. |

A single poll can produce **two** rows for the same stream if both the game and the title change between two consecutive polls.

### `twitch-silver/games`

Similar to the bronze games table, but with the IGDB struct flattened out into plain columns and array-of-struct fields reduced to array-of-string.
**Grain:** one row per (unique) game per poll.

| Field | Type |
|---|---|
| `ingestion_ts` | timestamp |
| `game_id` | string |
| `game_name` | string |
| `igdb_id` | string? |
| `summary` | string? |
| `total_rating` | float? |
| `total_rating_count` | integer? |
| `first_release_date` | timestamp? |
| `storyline` | string? |
| `url` | string? |
| `themes` | array<string>? |
| `player_perspectives` | array<string>? |
| `keywords` | array<string>? |
| `game_modes` | array<string>? |
| `platform_families` | array<string>? |
| `platform_types` | array<string>? |

Note this table is **not deduplicated**: the same game shows up again every time it's seen in a poll, with its IGDB data re-attached, because game data can change over time (particulary rating, themes, game modes, platforms etc. can be updated).  
Consumers are expected to pick the latest snapshot per `game_id` if they need a current-state view, i.e. the latest `ingestion_ts`.

---

## Gold layer (ClickHouse)

```mermaid
erDiagram
  fact_stream_hourly }o--|| dim_game : "refers to"
  fact_stream_hourly }o--|| dim_date : "occurs on"
  fact_stream_hourly }o--|| dim_streamer : "streamed by"
  fact_stream_hourly }o--|| dim_language : "broadcast in"

  fact_stream_hourly {
    Int64 game_id FK "dimension"
    UInt32 date_id FK "dimension"
    Int64 streamer_id FK "dimension"
    Int64 language_id FK "dimension"
    UInt8 time_hour "degenerate dimension"
    Int64 max_viewers "metric"
    Int64 sum_viewers "metric"
    Int32 count_observations "metric"
  }

  dim_game {
    Int64 game_id PK
    String game_name
    String? igdb_id
    Float64? total_rating
    Int64? total_rating_count
    DateTime? first_release_date
    Array(String)? themes
    Array(String)? keywords
    Array(String)? platforms
    DateTime inserted_at
  }

  dim_date {
    UInt32 date_id PK
    UInt16 date_year
    UInt8 date_month
    UInt8 date_day
    DateTime inserted_at
  }

  dim_streamer {
    Int64 streamer_id PK
    String user_name
    DateTime inserted_at
  }

  dim_language {
    Int64 language_id PK
    String language
    DateTime inserted_at
  }
```

A small star schema stored in ClickHouse data warehouse. Data is computed hourly starting from the streams and games silver tables (joined).

Surrogate keys for dimensions are generated with [`xxhash64`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.xxhash64.html) on the natural key, which is cheap (they are integers) and good enough at this scale; the date dimension is an exception, for which the unique integer ID is `YYYYMMDD`.

ClickHouse is append-only, and data is inserted in hourly increments, therefore it is critical to handle duplicate data by keeping the latest additions (we assume freshness implies correctness).

Dimension tables use the `ReplacingMergeTree` engine, so duplicate inserts of the same key are collapsed on merge (remember to query with `FINAL`, or run `OPTIMIZE ... FINAL`, if you need guaranteed deduplicated reads).

The only fact table uses the `SummingMergeTree` engine, so identical dimension combinations across merges get their measures summed automatically; this works because the measures are additive.

### `dim_game`

Not all games have IGDB data available (therefore the IGDB fields can be null).

| Column | Type | Notes |
|---|---|---|
| `game_id` | Int64 | `xxhash64(game_name)` |
| `game_name` | String | See silver games table. |
| `igdb_id` | String? | ^ |
| `total_rating` | Float64? | ^ |
| `total_rating_count` | Int64? | ^ |
| `first_release_date` | DateTime? | ^ |
| `themes` | Array(String)? | ^ |
| `keywords` | Array(String)? | ^ |
| `platforms` | Array(String)? | ^ |
| `inserted_at` | DateTime | Defaults to `now()`. |

### `dim_date`

| Column | Type | Notes |
|---|---|---|
| `date_id` | UInt32 | Formatted as `YYYYMMDD` (e.g. `20260707`) |
| `date_year` | UInt16 | |
| `date_month` | UInt8 | |
| `date_day` | UInt8 | |
| `inserted_at` | DateTime | Defaults to `now()`. |

### `dim_streamer`

| Column | Type | Notes |
|---|---|---|
| `streamer_id` | Int64 | `xxhash64(user_name)` |
| `user_name` | String | The streamer username. |
| `inserted_at` | DateTime | Defaults to `now()`. |

### `dim_language`

| Column | Type | Notes |
|---|---|---|
| `language_id` | Int64 | `xxhash64(language)` |
| `language` | String | ISO 639-1 language code, or the literal string `"other"`. |
| `inserted_at` | DateTime | Defaults to `now()`. |

### `fact_stream_hourly`

**Grain:** one row per (game, streamer, language, date, hour) dimension combination.

| Column | Notes |
|---|---|
| `game_id` | Foreign key to `dim_game`. |
| `date_id` | Foreign key to `dim_date`. |
| `streamer_id` | Foreign key to `dim_streamer`. |
| `language_id` | Foreign key to `dim_language`. |
| `time_hour` | Degenerate dimension (0–23), no full dimension table. |
| `max_viewers` | Max `viewer_count` observed in this hour bucket. |
| `sum_viewers` | Sum of `viewer_count` across all polls in the bucket (divide by `count_observations` to get an average). |
| `count_observations` | Number of polls that fell into this bucket; if the service is running correctly, this field should be 60 (one per minute in an hour). |

Rows with a blank `game_name` are filtered out before this table is populated (a stream with no category set can't be attached to a game
dimension), and missing `user_name` and `language` are filled with `"Unknown"` rather than dropped, so viewer numbers aren't silently lost (this is only a precaution, those fields should never be missing).