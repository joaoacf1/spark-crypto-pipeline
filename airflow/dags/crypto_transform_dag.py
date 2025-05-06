from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

with DAG('pyspark_example',
         default_args=default_args,
         schedule=None,
         description='DAG example with Pyspark') as dag:

    submit_job = SparkSubmitOperator(
        task_id='submit_pyspark_job',
        application='/opt/airflow/scripts/hello_spark.py',
        conn_id='spark_default',
        verbose=True,
        total_executor_cores=1,
        executor_cores=1,
        executor_memory='1g',
        driver_memory='1g',
        num_executors=1
    )
