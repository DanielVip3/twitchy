from common import *
from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
  dag_id='turin_dott_silver_transformation',
  default_args=default_args,
  description='Batch transformation (bronze to silver) every 30 minutes',
  schedule='*/30 * * * *',
  start_date=start_date,
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

turin_dott_silver_dag()