# airflow/dags/crypto_collect_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_collector import collect_data


# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['joaoantoniocorreaf@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define a DAG
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
        python_callable=collect_data,  # Função do script
        dag=dag,
    )

    collect_task  # Só uma tarefa (não precisa de >>)

