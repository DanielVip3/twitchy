from components.games.category_sorted_hbar import category_sorted_hbar
import polars as pl
import streamlit as st

def categories(theme_frequency: pl.DataFrame, game_mode_frequency: pl.DataFrame, platform_type_frequency: pl.DataFrame, player_perspective_frequency: pl.DataFrame, keyword_frequency: pl.DataFrame):
  with st.container(border=True):
    st.subheader("Themes, platforms, perspectives, keywords")
    c1, c2 = st.columns(2)

    with c1:
      category_sorted_hbar(theme_frequency, "theme", "Most common themes")
      category_sorted_hbar(game_mode_frequency, "game_mode", "Most common game modes")
    with c2:
      category_sorted_hbar(platform_type_frequency, "platform_type", "Most common platform types")
      category_sorted_hbar(player_perspective_frequency, "player_perspective", "Most common player perspectives")

    category_sorted_hbar(keyword_frequency, "keyword", "Most common keywords")