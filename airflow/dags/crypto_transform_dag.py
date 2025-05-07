from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['joaoantoniocorreaf@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

os.makedirs('/opt/airflow/data/raw', exist_ok=True)
os.makedirs('/opt/airflow/data/processed', exist_ok=True)

with DAG('crypto_spark_etl',
         default_args=default_args,
         schedule=None,
         description='Transform data with Spark') as dag:

    run_spark_etl = SparkSubmitOperator(
        task_id='run_spark_etl',
        application='/opt/airflow/scripts/spark_etl.py',
        conn_id='spark_default',
        verbose=True,
        conf={
            "spark.sql.files.ignoreCorruptFiles": "true",
            "spark.sql.files.ignoreMissingFiles": "true",
            "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:///opt/airflow/config/log4j.properties"
        },
        total_executor_cores=1,
        executor_cores=1,
        executor_memory='2g',
        driver_memory='2g',
        num_executors=1
    )

    run_spark_etl