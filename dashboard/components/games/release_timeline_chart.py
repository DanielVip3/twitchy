import polars as pl
import streamlit as st
import plotly.express as px
from theme import TWITCH_PURPLE, ACCENT

def release_timeline_chart(trend: pl.DataFrame):
  with st.container(border=True):
    st.subheader("Release timeline")

    if trend.is_empty():
      st.info("No release date data available yet.")
      return
    pdf = trend.to_pandas()
    fig = px.bar(
      pdf,
      x="release_year",
      y="games_released",
      title="Games tracked by release year",
      color_discrete_sequence=[TWITCH_PURPLE],
    )
    fig.add_scatter(
      x=pdf["release_year"],
      y=pdf["avg_rating"],
      mode="lines+markers",
      name="Avg. rating",
      yaxis="y2",
      line=dict(color=ACCENT),
    )
    fig.update_layout(
      yaxis=dict(title="Games released"),
      yaxis2=dict(title="Avg. rating", overlaying="y", side="right", range=[0, 100]),
      legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, width='stretch')