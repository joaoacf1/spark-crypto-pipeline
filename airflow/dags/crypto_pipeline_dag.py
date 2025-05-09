from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from scripts.data_collector import collect_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['joaoantoniocorreaf@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

with DAG(
    'crypto_pipeline',
    default_args=default_args,
    description='Complete crypto data pipeline: collect, process and store data',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'binance', 'spark', 's3'],
    max_active_runs=1
) as dag:

    collect_task = PythonOperator(
        task_id='collect_binance_data',
        python_callable=collect_data,
    )

    spark_task = SparkSubmitOperator(
        task_id='process_with_spark',
        application='/opt/airflow/scripts/spark_etl.py',
        conn_id='spark_default',
        verbose=True,
        conf={
            "spark.sql.files.ignoreCorruptFiles": "true",
            "spark.sql.files.ignoreMissingFiles": "true",
            "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:///opt/airflow/config/log4j.properties",
            "spark.executor.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true"
        },
        executor_cores=1,
        executor_memory='2g',
        driver_memory='2g',
        num_executors=1,
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.180"
    )

    collect_task >> spark_task 