import streamlit as st
import components
from helpers.streams import latest_snapshot
from helpers.games import latest_games_snapshot, top_rated_games,theme_frequency,game_mode_frequency, player_perspective_frequency, platform_type_frequency, keyword_frequency, release_period_trend, rating_vs_popularity, rating_popularity_quadrants
from load_data import load_games, load_streams

st.set_page_config(page_title="Game Insights", layout="wide")
st.title("Game insights")

with st.spinner("Loading game and stream data..."):
  games_raw = load_games()
  streams = load_streams()

if games_raw.is_empty():
  st.warning("No game data available yet.")
  st.stop()

games = latest_games_snapshot(games_raw)
latest_streams = latest_snapshot(streams)

# ---- Sidebar filters ----

st.sidebar.header("Filters")
top_n = st.sidebar.slider("Top N", 5, 30, 10, key="games_top_n")
min_rating_count = st.sidebar.slider("Minimum ratings count", 0, 100, 5)


# ---- Components ----

components.games_kpis(games)

components.rating_distribution(games, top_rated_games(games, top_n, min_rating_count))

components.categories(
  theme_frequency(games, top_n),
  game_mode_frequency(games, top_n),
  platform_type_frequency(games, top_n),
  player_perspective_frequency(games, top_n),
  keyword_frequency(games, top_n)
)

components.release_timeline_chart(release_period_trend(games))

merged = rating_vs_popularity(games, latest_streams)
if merged.is_empty():
  st.info("Not enough overlapping game names between IGDB and live Twitch data yet.")
else:
  quadrants = rating_popularity_quadrants(merged)
  
  components.rating_popularity(quadrants, top_n)

components.games_explorer(games)