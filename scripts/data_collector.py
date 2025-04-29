import pandas as pd
import requests
import logging
from datetime import datetime, timezone
import os

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
          
def save_to_csv(path, df):
    try:
        df.to_csv(path, index=False)
        logging.info(f"Data saved successfully in {path}")
    except Exception as e:
        logging.error(f"Error when saving data: {e}")
        raise

if __name__ == '__main__':
    raw_dir = os.path.join(base_dir, 'data', 'raw')
    timestamp = datetime.now().strftime('%Y%m%dT%H%M')
    raw_file_path = os.path.join(raw_dir, f"raw_crypto_prices_{timestamp}.csv")

    os.makedirs(raw_dir, exist_ok=True)

    df = fetch_prices()
    save_to_csv(raw_file_path, df)
