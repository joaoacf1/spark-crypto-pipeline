from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['joaoantoniocorreaf@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('crypto_spark_etl',
         default_args=default_args,
         schedule=None,
         description='Transform data with Spark') as dag:

    run_spark_etl = SparkSubmitOperator(
        task_id='run_spark_etl',
        application='/opt/airflow/scripts/spark_etl.py',
        conn_id='spark_default',
        verbose=True,
        total_executor_cores=1,
        executor_cores=1,
        executor_memory='1g',
        driver_memory='1g',
        num_executors=1
    )

    run_spark_etl