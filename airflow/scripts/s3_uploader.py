import os
import logging
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)
logger = logging.getLogger(__name__)

def upload_directory_to_s3(directory, bucket, prefix=""):
    """
    Upload all parquet files from a directory to S3.
    
    Args:
        directory (str): Local directory containing parquet files
        bucket (str): S3 bucket name
        prefix (str): S3 key prefix
    """
    if not os.path.exists(directory):
        raise ValueError(f"Directory {directory} does not exist")

    s3 = boto3.client('s3',
                     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                     region_name=os.getenv("AWS_DEFAULT_REGION"))

    # Check if bucket exists
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            raise ValueError(f"Bucket {bucket} does not exist")
        elif error_code == '403':
            raise ValueError(f"Access denied to bucket {bucket}")
        else:
            raise

    uploaded_files = []
    failed_files = []

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".parquet"):
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, directory)
                s3_path = os.path.join(prefix, relative_path).replace("\\", "/")

                try:
                    s3.upload_file(local_path, bucket, s3_path)
                    logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_path}")
                    uploaded_files.append(file)
                except Exception as e:
                    logger.error(f"Error uploading {file}: {str(e)}")
                    failed_files.append(file)

    return {
        "uploaded_files": uploaded_files,
        "failed_files": failed_files,
        "total_uploaded": len(uploaded_files),
        "total_failed": len(failed_files)
    }

if __name__ == "__main__":
    load_dotenv()
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parquet_dir = os.path.join(base_dir, "data", "processed")
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME environment variable not set")
    
    result = upload_directory_to_s3(parquet_dir, bucket_name, prefix="processed/")
    
    print(f"\nUpload Summary:")
    print(f"Total files uploaded: {result['total_uploaded']}")
    print(f"Total files failed: {result['total_failed']}")
    
    if result['failed_files']:
        print("\nFailed files:")
        for file in result['failed_files']:
            print(f"- {file}")
