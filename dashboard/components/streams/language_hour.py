import polars as pl
import streamlit as st
import plotly.express as px

def language_hour(latest: pl.DataFrame):
  with st.container(border=True):
    c3, c4 = st.columns(2)

    with c3:
      st.subheader("Streams by language")

      lang_df = latest \
        .group_by("language") \
        .agg(pl.len().alias("streams")) \
        .sort("streams", descending=True)
      
      fig = px.pie(lang_df.to_pandas(), names="language", values="streams", hole=0.4)
      st.plotly_chart(fig, width="stretch")

    with c4:
      st.subheader("Streams by starting hour")

      hour_df = latest \
        .group_by("started_hour") \
        .agg(pl.len().alias("streams")) \
        .sort("started_hour")
      
      fig = px.line(hour_df.to_pandas(), x="started_hour", y="streams")
      st.plotly_chart(fig, width="stretch")