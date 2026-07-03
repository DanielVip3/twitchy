from common import get_spark_session
from haversine import haversine
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
import pandas as pd

NOISE_THRESHOLD_METERS = 300.0    # under 300m distance, the bike is considered still (GPS can jitter)
NOISE_THRESHOLD_DURATION = 2      # under 2 minutes, the bike is considered still
NOISE_THRESHOLD_SPEED_KM_H = 4    # under 4 km/h, the bike is considered still
MAX_MISSING_HOURS = 6             # after 6 hours of missing, the bike is considered abandoned
POLL_TIMEOUT_MINUTES = 7          # every 7 minutes we check for the state of bikes (the bronze ingests every 3 minutes)

spark = get_spark_session("DotTurinSilverTrips")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
spark.conf.set("spark.sql.adaptive.enabled", "false")

trips_schema = StructType([
  StructField("bike_id", StringType()),
  StructField("vehicle_type_id", StringType()),
  StructField("start_ts", TimestampType()),
  StructField("end_ts", TimestampType()),
  StructField("duration_s", IntegerType()),
  StructField("start_lat", DoubleType()),
  StructField("start_lon", DoubleType()),
  StructField("end_lat", DoubleType()),
  StructField("end_lon", DoubleType()),
  StructField("distance_m", DoubleType()),
  StructField("fuel_start", DoubleType()),
  StructField("fuel_end", DoubleType())
])
trips_columns = [f.name for f in trips_schema.fields]

# State schema for each bike_id
# status is either "active" or "missing"
# last_* are the current state
# pending_* are used to store the initial state of a pending trip
state_schema = "status string, " \
  "last_seen timestamp, " \
  "last_lat double, " \
  "last_lon double, " \
  "last_fuel double, " \
  "vehicle_type_id string, " \
  "pending_start_ts timestamp, " \
  "pending_start_lat double, " \
  "pending_start_lon double, " \
  "pending_start_fuel double"

def update_bike_state(grouping_key, dfs, state: GroupState):
  bike_id = grouping_key[0]
  trips = []

  # Case 1: no new data, but state has timed out, i.e. bike is not found anymore (missing)
  # It can mean that a trip is ongoing, and it can be ongoing up to MAX_MISSING_HOURS.
  if state.hasTimedOut:
    if state.exists:
      old = state.get
      status = old[0]

      # Case 1.1: it was "active" and now it is missing
      if status == "active":
        # Mark as "missing"
        new_state = (
          "missing", old[1], old[2], old[3], old[4], old[5],
          old[1], old[2], old[3], old[4]  # pending_* = last known state
        )
        state.update(new_state)

        # Wait again for a higher missing timeout to mark the bike as abandoned
        state.setTimeoutDuration(MAX_MISSING_HOURS * 60 * 60 * 1000)
      
      # Case 1.2: it was already "missing" so the bike is abandoned -> clean the state
      else:
        state.remove()

    return []

  # Case 2: new data for this bike_id
  for df in dfs:
    df = df.sort_values("last_reported")

    for _, row in df.iterrows():
      # Case 2.1: this is a never seen bike
      if not state.exists:
        state.update((
          "active", row["last_reported"], row["lat"], row["lon"],
          row["current_fuel_percent"], row["vehicle_type_id"],
          None, None, None, None
        ))

      else:
        old = state.get
        status = old[0]
        
        # Case 2.2: this bike was missing and now reappared -> close the trip
        # It means that a trip correctly happened within the MAX_MISSING_HOURS hours limit.
        if status == "missing":
          dist = haversine((old[7], old[8]), (row["lat"], row["lon"]), unit='m')
          duration_s = (row["last_reported"] - old[6]).total_seconds()
          duration_m = duration_s / 60.0
          if duration_s == 0 or duration_m == 0:
            avg_speed_km_h = 0
          else:
            avg_speed_km_h = (dist / 1000.0) / (duration_m / 60.0)

          # A trip happens only if the distance and duration are enough to not
          # be noise or GPS jitter.
          if dist > NOISE_THRESHOLD_METERS and duration_m > NOISE_THRESHOLD_DURATION and avg_speed_km_h > NOISE_THRESHOLD_SPEED_KM_H:
            trips.append({
              "bike_id": bike_id,
              "vehicle_type_id": row["vehicle_type_id"],
              "start_ts": old[6],
              "end_ts": row["last_reported"],
              "duration_s": duration_s,
              "start_lat": old[7],
              "start_lon": old[8],
              "end_lat": row["lat"],
              "end_lon": row["lon"],
              "distance_m": dist,
              "fuel_start": old[9],
              "fuel_end": row["current_fuel_percent"]
            })

          state.update((
            "active", row["last_reported"], row["lat"], row["lon"],
            row["current_fuel_percent"], row["vehicle_type_id"],
            None, None, None, None
          ))
        
        # Case 2.3: this bike was already active, not missing
        else:
          dist = haversine((old[2], old[3]), (row["lat"], row["lon"]), unit='m')
          duration_s = (row["last_reported"] - old[1]).total_seconds()
          duration_m = duration_s / 60.0
          if duration_s == 0 or duration_m == 0:
            avg_speed_km_h = 0
          else:
            avg_speed_km_h = (dist / 1000.0) / (duration_m / 60.0)

          # A trip happens only if the distance and duration are enough to not
          # be noise or GPS jitter.
          if dist > NOISE_THRESHOLD_METERS and duration_m > NOISE_THRESHOLD_DURATION and avg_speed_km_h > NOISE_THRESHOLD_SPEED_KM_H:
            trips.append({
              "bike_id": bike_id,
              "vehicle_type_id": row["vehicle_type_id"],
              "start_ts": old[1],
              "end_ts": row["last_reported"],
              "duration_s": duration_s,
              "start_lat": old[2],
              "start_lon": old[3],
              "end_lat": row["lat"],
              "end_lon": row["lon"],
              "distance_m": dist,
              "fuel_start": old[4],
              "fuel_end": row["current_fuel_percent"]
            })
          
          state.update((
            "active", row["last_reported"], row["lat"], row["lon"],
            row["current_fuel_percent"], row["vehicle_type_id"],
            None, None, None, None
          ))
    
  # Case 3: no new data for this bike_id, and no time out. The bike is still free. Do nothing.

  # The state timeouts after POLL_TIMEOUT_MINUTES if the bike is active.
  # In other words, we check every POLL_TIMEOUT_MINUTES for its state.
  state.setTimeoutDuration(POLL_TIMEOUT_MINUTES * 60 * 1000)

  if trips:
    return [pd.DataFrame(trips, columns=trips_columns)]
  else:
    return []

# Read Delta Lake from the bronze layer bucket and select only useful columns
bronze_stream_df = spark.readStream \
  .format("delta") \
  .load("s3a://dotturin-raw/bikes_status/") \
  .select("bike_id", "vehicle_type_id", "lat", "lon", "current_fuel_percent", "last_reported")

# Create the trips table, grouping by bike_id and applying the state management logic
trips_df = bronze_stream_df.groupBy("bike_id").applyInPandasWithState(
  update_bike_state,
  outputStructType=trips_schema,
  stateStructType=state_schema,
  outputMode="append",
  timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
)

# Write Delta Lake in the processed bucket
query = trips_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "s3a://dotturin-processed/checkpoints/silver_trips/") \
  .start("s3a://dotturin-processed/trips/")

query.awaitTermination()