import pandas as pd
import requests
import logging
from datetime import datetime, timezone
import os
import boto3
from io import StringIO

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

os.makedirs(os.path.join(base_dir, 'logs'), exist_ok=True)

logging.basicConfig(
    filename=os.path.join(base_dir, 'logs', 'collector.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def fetch_prices():
    try:
        response = requests.get("https://api.binance.com/api/v3/ticker/price")
        data = response.json()
        df = pd.DataFrame(data)
        df["timestamp"] = datetime.now(timezone.utc)
        logging.info(f"{len(df)} symbols collected from Binance")
        return df
    except Exception as e:
        logging.error(f"Data extraction error: {e}")
        raise
          
def save_to_s3(df, bucket_name, object_key):
    try:
        s3_client = boto3.client('s3')
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=csv_buffer.getvalue()
        )
        logging.info(f"Data successfully saved to S3 bucket: {bucket_name}/{object_key}")
    except Exception as e:
        logging.error(f"Error when saving data to S3: {e}")
        raise
    

def collect_data():
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M')
    
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    if not bucket_name:
        logging.error("S3_BUCKET_NAME environment variable is not set")
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    
    object_key = f"raw/raw_crypto_prices_{timestamp}.csv"
    
    df = fetch_prices()
    
    save_to_s3(df, bucket_name, object_key)

if __name__ == '__main__':
    collect_data()