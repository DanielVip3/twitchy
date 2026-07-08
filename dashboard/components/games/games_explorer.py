import polars as pl
import streamlit as st

def games_explorer(games: pl.DataFrame):
  with st.container(border=True):
    st.subheader("Game explorer")
    st.caption("Browse the IGDB games catalog, filtered by theme or platform.")

    all_themes = sorted(
      games.select("themes") \
        .explode("themes") \
        .drop_nulls() \
        .unique() \
        .to_series() \
        .to_list()
    )

    all_platforms = sorted(
      games.select("platforms") \
        .explode("platforms") \
        .drop_nulls() \
        .unique() \
        .to_series() \
        .to_list()
    )

    c1, c2 = st.columns(2)
    theme_filter = c1.multiselect("Theme", all_themes)
    platform_filter = c2.multiselect("Platform", all_platforms)

    filtered = games
    if theme_filter:
      filtered = filtered.filter(pl.col("themes").list.eval(pl.element().is_in(theme_filter)).list.any())
    if platform_filter:
      filtered = filtered.filter(pl.col("platforms").list.eval(pl.element().is_in(platform_filter)).list.any())

    df = filtered.select("game_name", "total_rating", "total_rating_count", "themes", "platforms", "game_modes").to_pandas()
    st.dataframe(
      data=df,
      width='stretch',
      hide_index=True,
    )