import polars as pl
import streamlit as st
import plotly.express as px
from datetime import datetime
from collections.abc import Callable

EVENT_COLORS = {
  "GAME_CHANGE": "#FF6B6B",
  "TITLE_CHANGE": "#4ECDC4"
}

def events(transitions: pl.DataFrame, top_n: int, format_datetime: Callable[[datetime], str]):
  with st.container(border=True):
    st.subheader("Stream events")

    if not transitions.is_empty():
      c7, c8 = st.columns(2)

      with c7:
        st.caption("Event types")
        event_counts = transitions \
          .group_by("event_type") \
          .agg(pl.len().alias("events"))

        fig = px.pie(
          event_counts.to_pandas(),
          names="event_type",
          values="events",
          hole=0.5,
          color="event_type",
          color_discrete_map=EVENT_COLORS
        )

        st.plotly_chart(fig, width="stretch")
      
      with c8:
        st.caption(f"{top_n} most recent events")
        
        if not transitions.is_empty():
          recent = transitions.sort("event_ts", descending=True).head(10)
          
          for row in recent.to_dicts():
            event_type = row["event_type"]
            color = EVENT_COLORS.get(event_type, "#999999")
            
            if event_type == "TITLE_CHANGE":
              change_text = f"{row['old_value']} <br/> → {row['new_value']}"
            elif event_type == "GAME_CHANGE":
              change_text = f"{row['old_value']} <br/> → {row['new_value']}"

            st.write(
              f"""
              <div style="
                padding: 12px;
                margin: 8px 0;
                border-left: 4px solid {color};
                background-color: rgba(0,0,0,0.02);
                border-radius: 4px;
              ">
                <strong style="color: {color};">{event_type.replace("_", " ")}</strong> · 
                <span style="font-weight: 500;">{row['user_name']}</span> · 
                <span style="color: #666;">{format_datetime(row['event_ts'])}</span><br/>
                <span style="font-size: 0.9em; color: #555;">{change_text}</span>
              </div>
              """,
              unsafe_allow_html=True
            )
        else:
          st.info("No events yet.")
    else:
      st.info("No transition events yet.")