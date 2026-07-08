import polars as pl
import streamlit as st

def games_kpis(games: pl.DataFrame):
  total_games = games.height
  with_igdb = games.filter(pl.col("igdb_id").is_not_null()).height
  coverage_pct = (with_igdb / total_games * 100) if total_games else 0.0

  rated = games.filter(pl.col("total_rating").is_not_null())
  avg_rating = rated["total_rating"].mean() if not rated.is_empty() else None

  most_reviewed = games.filter(pl.col("total_rating_count").is_not_null()) \
    .sort("total_rating_count", descending=True) \
    .head(1)

  most_reviewed_name = most_reviewed["game_name"][0] if not most_reviewed.is_empty() else None
  most_reviewed_count = most_reviewed["total_rating_count"][0] if not most_reviewed.is_empty() else None

  with st.container(border=True):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Games tracked", f"{total_games:,}")
    c2.metric("IGDB coverage", f"{coverage_pct:.0f}%")
    c3.metric("Avg. rating", f"{avg_rating:.1f}" if avg_rating is not None else "N/A")
    if most_reviewed_name:
      c4.metric("Most reviewed game", most_reviewed_name, f"{most_reviewed_count:,} ratings")
    else:
      c4.metric("Most reviewed game", "N/A")