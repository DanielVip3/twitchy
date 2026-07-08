from components.streams.sorted_hbar import sorted_hbar
import polars as pl
import streamlit as st

def tags(top_tags_by_frequency: pl.DataFrame, top_tags_by_viewers: pl.DataFrame):
  with st.container(border=True):
    st.subheader("Tags (in top 100 streams)")

    c5, c6 = st.columns(2)

    with c5:
      st.caption("Most used tags")
      st.plotly_chart(sorted_hbar(top_tags_by_frequency, "uses", "tag_name"), width="stretch")

    with c6:
      st.caption("Tags associated with the most viewers")
      st.plotly_chart(sorted_hbar(top_tags_by_viewers, "avg_viewers", "tag_name"), width="stretch")