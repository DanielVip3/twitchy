from common import *
from airflow.sdk import dag, task

@dag(
  dag_id='turin_dott_api_ingestion',
  default_args=default_args,
  description='3 minutes ingestion flow for Turin Dott API',
  schedule='*/3 * * * *',
  start_date=start_date,
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

turin_dott_api_ingestion_dag()