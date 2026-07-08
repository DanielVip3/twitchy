import polars as pl
import plotly.express as px
from theme import TWITCH_PURPLE

def sorted_hbar(df: pl.DataFrame, x: str, y: str, color: str | None = None):
  """
  Build a horizontal bar graph sorted by highest to lowest values.
  """

  fig = px.bar(
    df.to_pandas(),
    x=x,
    y=y,
    orientation="h",
    color=color,
    color_discrete_sequence=[TWITCH_PURPLE]
  )
  
  fig.update_layout(yaxis={"categoryorder": "total ascending"})
  return fig