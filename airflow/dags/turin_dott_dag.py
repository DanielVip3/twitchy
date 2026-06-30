import pendulum
from datetime import timedelta
from airflow.sdk import dag, task

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
  @task.virtualenv(
    task_id='run_kafka_producer',
    requirements=['confluent-kafka', 'requests']
  )
  def trigger_producer():
    import subprocess
    import sys
    
    subprocess.run([sys.executable, '-u', '/opt/airflow/scripts/producer.py'], check=True)

  # Run the producer Python script
  trigger_producer()

turin_dott_api_ingestion_dag()