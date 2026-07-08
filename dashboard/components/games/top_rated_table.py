import polars as pl
import streamlit as st

def top_rated_table(top_rated: pl.DataFrame):
  st.markdown("**Top rated games**")

  if top_rated.is_empty():
    st.info("No games meet the minimum rating count.")
    return

  st.dataframe(
    top_rated.to_pandas(),
    width='stretch',
    hide_index=True,
    column_config={
      "url": st.column_config.LinkColumn("IGDB page")
    },
  )