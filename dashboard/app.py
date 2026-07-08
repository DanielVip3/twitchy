import streamlit as st
import polars as pl
import components
from helpers.streams import latest_snapshot, top_games, top_streamers, top_tags_by_frequency, top_tags_by_viewers, format_datetime
from load_data import load_streams, load_tags, load_transitions
from datetime import datetime, timedelta

st.set_page_config(page_title="Twitch Dashboard", layout="wide")

st.title("Streams insights")

with st.spinner("Loading streams..."):
  streams = load_streams()
  tags = load_tags()
  transitions = load_transitions()

if streams.is_empty():
  st.warning("No data available.")
  st.stop()

all_games = sorted(streams["game_name"].drop_nulls().unique().to_list())


# ---- Sidebar filters ----

max_time = streams["ingestion_ts"].max()
min_time = streams["ingestion_ts"].min()
 
selected_date = st.sidebar.date_input(
  "Select date",
  value=max_time.date(),
  min_value=min_time.date(),
  max_value=max_time.date()
)
 
selected_time = st.sidebar.time_input(
  "Select time",
  value=max_time.time(),
  step=timedelta(minutes=1)
)

selected_datetime = datetime.combine(selected_date, selected_time)

st.sidebar.caption(f"Viewing: {format_datetime(selected_datetime)}")

selected_games = st.sidebar.multiselect("Filter by game", all_games)

top_n = st.sidebar.slider("Top N", 1, 20, 10)

if selected_games:
  # Only applied to the streams table, tags/transitions stay unfiltered [TODO: revise]
  streams = streams.filter(pl.col("game_name").is_in(selected_games))

latest = latest_snapshot(streams, selected_datetime)


# ---- Components ----

components.streams_kpis(streams, latest)

components.top_games_streamers(top_games(latest, top_n), top_streamers(streams, latest, top_n))

components.viewer_trend(streams, top_streamers(streams, latest, top_n))

components.language_hour(latest)

components.tags(top_tags_by_frequency(tags, top_n), top_tags_by_viewers(tags, streams, top_n))

components.events(transitions, top_n, format_datetime)