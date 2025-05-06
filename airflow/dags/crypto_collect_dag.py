from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
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
}

with DAG(
    dag_id='crypto_collect',
    default_args=default_args,
    description='Collects data from Binance and saves it in CSV',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'binance', 'collect'],
) as dag:

    collect_task = PythonOperator(
        task_id='collect_binance_data',
        python_callable=collect_data,  
        dag=dag,
    )

    collect_task 

