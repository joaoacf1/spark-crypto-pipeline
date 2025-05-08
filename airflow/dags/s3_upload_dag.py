from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from scripts.s3_uploader import upload_directory_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

def upload_to_s3(**context):
    """Task to upload files to S3"""
    bucket = os.getenv('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable not set")
    
    result = upload_directory_to_s3(
        directory='/opt/airflow/data/processed',
        bucket=bucket,
        prefix='processed/'
    )
    
    if result['total_failed'] > 0:
        raise Exception(f"Failed to upload {result['total_failed']} files. Check logs for details.")
    
    return result

with DAG(
    's3_upload_dag',
    default_args=default_args,
    description='Upload processed files to S3',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['s3', 'upload'],
    max_active_runs=1
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    upload_task 