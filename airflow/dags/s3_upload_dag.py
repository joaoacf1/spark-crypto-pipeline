from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from scripts.s3_uploader import upload_directory_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    's3_upload_dag',
    default_args=default_args,
    description='Upload processed files to S3',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['s3', 'upload'],
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_directory_to_s3,
        op_kwargs={
            'directory': '/opt/airflow/data/processed',
            'bucket': os.getenv('S3_BUCKET_NAME', ''),
            'prefix': 'processed/'
        }
    )

    upload_task 