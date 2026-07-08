import os
from dotenv import load_dotenv
import streamlit as st
import polars as pl

load_dotenv()

STORAGE_OPTIONS = {
  "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER"),
  "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD"),
  "AWS_ENDPOINT_URL": "http://localhost:9000",
  "AWS_REGION": "us-east-1",
  "AWS_ALLOW_HTTP": "true"
}

def _read_delta(path: str) -> pl.DataFrame:
  try:
    return pl.read_delta(path, storage_options=STORAGE_OPTIONS)
  except Exception as e:
    st.error(f"Error loading {path}: {e}")
    return pl.DataFrame()


@st.cache_data(ttl=60)
def load_streams() -> pl.DataFrame:
  return _read_delta("s3://twitch-silver/streams/")


@st.cache_data(ttl=60)
def load_tags() -> pl.DataFrame:
  return _read_delta("s3://twitch-silver/stream_tags/")


@st.cache_data(ttl=60)
def load_transitions() -> pl.DataFrame:
  return _read_delta("s3://twitch-silver/stream_transitions/")


@st.cache_data(ttl=60)
def load_games() -> pl.DataFrame:
  return _read_delta("s3://twitch-silver/games/")