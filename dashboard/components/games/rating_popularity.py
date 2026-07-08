import polars as pl
import streamlit as st
import plotly.express as px
from theme import QUADRANT_COLORS

def rating_popularity(quadrants: pl.DataFrame, n: int):
  with st.container(border=True):
    st.subheader("Critical acclaim vs. streaming popularity")

    fig = px.scatter(
      quadrants.to_pandas(),
      x="current_viewers",
      y="total_rating",
      color="quadrant",
      color_discrete_map=QUADRANT_COLORS,
      hover_name="game_name",
      hover_data=["total_rating_count", "live_streams"],
      log_x=True,
      title="Critical acclaim vs. current Twitch popularity",
    )
    fig.update_layout(xaxis_title="Current Twitch viewers (log)", yaxis_title="Rating")

    st.plotly_chart(fig, width='stretch')

    c1, c2 = st.columns(2)
    with c1:
      st.markdown("**Hidden gems** (high rating, low current viewership)")
      gems = quadrants \
        .filter(pl.col("quadrant") == "Hidden gem") \
        .sort("total_rating", descending=True) \
        .head(n)

      st.dataframe(
        gems.select("game_name", "total_rating", "current_viewers", "live_streams").to_pandas(),
        width='stretch',
        hide_index=True,
      )
    with c2:
      st.markdown("**Overhyped** (high viewership, low rating)")
      overhyped = quadrants \
        .filter(pl.col("quadrant") == "Overhyped") \
        .sort("current_viewers", descending=True) \
        .head(n)
      
      st.dataframe(
        overhyped.select("game_name", "total_rating", "current_viewers", "live_streams").to_pandas(),
        width='stretch',
        hide_index=True,
      )