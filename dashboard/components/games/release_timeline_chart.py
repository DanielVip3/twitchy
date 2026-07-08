import polars as pl
import streamlit as st
import plotly.express as px
from theme import TWITCH_PURPLE

def release_timeline_chart(trend: pl.DataFrame):
  with st.container(border=True):
    st.subheader("Release timeline")

    if trend.is_empty():
      st.info("No release date data available yet.")
      return

    df = trend.to_pandas()

    fig = px.bar(
      data_frame=df,
      x="period",
      y="games_released",
      color="avg_rating",
      color_continuous_scale=["#D9D9E3", TWITCH_PURPLE],
      range_color=[0, 100],
      title="Games tracked by release period",
      category_orders={"period": df["period"].tolist()},
    )

    fig.update_layout(
      xaxis_title="Release period",
      yaxis_title="Games tracked",
      coloraxis_colorbar=dict(title="Avg. rating"),
    )
    
    st.plotly_chart(fig, width='stretch')