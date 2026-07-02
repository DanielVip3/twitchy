import pendulum
from datetime import timedelta
import os

SPARK_APP_CORES = os.environ.get("SPARK_APP_CORES", "1")

default_args = {
  'owner': 'root',
  'depends_on_past': False,
  'retries': 1,
  'retry_delay': timedelta(seconds=30),
}

start_date = pendulum.datetime(2026, 6, 30, tz="UTC")