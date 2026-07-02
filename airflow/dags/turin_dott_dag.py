import os
import pendulum
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_APP_CORES = os.environ.get("SPARK_APP_CORES", "1")

default_args = {
  'owner': 'root',
  'depends_on_past': False,
  'retries': 1,
  'retry_delay': timedelta(seconds=30),
}

@dag(
  dag_id='turin_dott_api_ingestion',
  default_args=default_args,
  description='3 minutes ingestion flow for Turin Dott API',
  schedule='*/3 * * * *',
  start_date=pendulum.datetime(2026, 6, 30, tz="UTC"),
  catchup=False
)
def turin_dott_api_ingestion_dag():
  @task(task_id='run_kafka_producer')
  def trigger_producer():
    import subprocess
    import sys
    
    subprocess.run([sys.executable, '-u', '/opt/airflow/scripts/producer.py'], check=True)

  # Run the producer Python script
  trigger_producer()

@dag(
  dag_id='turin_dott_silver_transformation',
  default_args=default_args,
  description='Batch transformation (bronze to silver) every 30 minutes',
  schedule='*/30 * * * *',
  start_date=pendulum.datetime(2026, 6, 30, tz="UTC"),
  catchup=False
)
def turin_dott_silver_dag():
  run_silver = SparkSubmitOperator(
    task_id='run_silver',
    application='/opt/airflow/scripts/transform_silver.py',
    conn_id='spark_default',
    conf={
      'spark.cores.max': SPARK_APP_CORES
    },
    verbose=True
  )

turin_dott_api_ingestion_dag()
turin_dott_silver_dag()