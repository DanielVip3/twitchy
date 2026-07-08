from components.streams.sorted_hbar import sorted_hbar
import polars as pl
import streamlit as st
import plotly.express as px

def top_games_streamers(top_games: pl.DataFrame, top_streamers: pl.DataFrame):
  with st.container(border=True):
    c1, c2 = st.columns(2)

    with c1:
      st.subheader("Top games right now (among top 100 streams)")
      st.plotly_chart(sorted_hbar(top_games, "total_viewers", "game_name", "streams"), width="stretch")

    with c2:
      st.subheader("Top online streamers in last 10 min. by average, current and peak viewers")

      reshaped = pl.concat([
        top_streamers.select("user_name", pl.lit("latest").alias("metric"), "latest_viewer_count").rename({"latest_viewer_count": "viewers"}),
        top_streamers.select("user_name", pl.lit("peak").alias("metric"), "peak_viewers").rename({"peak_viewers": "viewers"}),
        top_streamers.select("user_name", pl.lit("avg").alias("metric"), "avg_viewers_10").rename({"avg_viewers_10": "viewers"})
      ])

      fig = px.bar(reshaped.to_pandas(), x="viewers", y="user_name", color="metric", orientation="h", barmode="overlay")
      fig.update_layout(yaxis={"categoryorder": "total ascending"})
      st.plotly_chart(fig, width="stretch")