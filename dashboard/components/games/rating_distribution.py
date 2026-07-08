from components.games.top_rated_table import top_rated_table
import polars as pl
import streamlit as st
import plotly.express as px
from theme import TWITCH_PURPLE

def rating_distribution(games: pl.DataFrame, top_rated: pl.DataFrame):
  with st.container(border=True):
    st.subheader("Ratings")
    c1, c2 = st.columns(2)

    rated = games.filter(pl.col("total_rating").is_not_null() & pl.col("total_rating_count").is_not_null())

    if rated.is_empty():
      st.info("No rating data available yet.")
      return

    with c1:
      fig = px.histogram(
        rated.to_pandas(),
        x="total_rating",
        nbins=30,
        title="Distribution of ratings",
        color_discrete_sequence=[TWITCH_PURPLE],
      )
      fig.update_layout(xaxis_title="Rating", yaxis_title="Number of games")

      st.plotly_chart(fig, width='stretch')

    with c2:
      fig = px.scatter(
        rated.to_pandas(),
        x="total_rating_count",
        y="total_rating",
        hover_name="game_name",
        log_x=True,
        title="Rating vs. number of ratings (log scale)",
        color_discrete_sequence=[TWITCH_PURPLE],
      )
      fig.update_layout(xaxis_title="Number of ratings (log)", yaxis_title="Rating")

      st.plotly_chart(fig, width='stretch')
    
    top_rated_table(top_rated)