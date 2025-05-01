import os
import logging
import boto3
from dotenv import load_dotenv

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path=dotenv_path)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

print(f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}")
print(f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}")
print(f"AWS_DEFAULT_REGION={AWS_DEFAULT_REGION}")
print(f"S3_BUCKET_NAME={S3_BUCKET_NAME}")

logging.basicConfig(
    filename=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 's3_uploader.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def upload_directory_to_s3(directory, bucket, prefix=""):
    s3 = boto3.client('s3',
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_DEFAULT_REGION)

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".parquet"):
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, directory)
                s3_path = os.path.join(prefix, relative_path).replace("\\", "/")

                try:
                    s3.upload_file(local_path, bucket, s3_path)
                    logging.info(f"Uploaded {local_path} to s3://{bucket}/{s3_path}")
                    print(f"✔️ {file} enviado com sucesso!")
                except Exception as e:
                    logging.error(f"Erro ao enviar {file}: {e}")
                    print(f"❌ Falha ao enviar {file}: {e}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parquet_dir = os.path.join(base_dir, "data", "processed")
    upload_directory_to_s3(parquet_dir, S3_BUCKET_NAME, prefix="processed/")
