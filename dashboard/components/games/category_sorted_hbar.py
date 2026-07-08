import polars as pl
import streamlit as st
import plotly.express as px
from theme import TWITCH_PURPLE

def category_sorted_hbar(freq: pl.DataFrame, category_col: str, title: str):
  if freq.is_empty():
    st.info(f"No data for: {title}")
    return

  fig = px.bar(
    freq.to_pandas(),
    x="count",
    y=category_col,
    orientation="h",
    title=title,
    color_discrete_sequence=[TWITCH_PURPLE],
  )
  fig.update_layout(yaxis={"categoryorder": "total ascending"}, xaxis_title="Games", yaxis_title="")

  st.plotly_chart(fig, width='stretch')