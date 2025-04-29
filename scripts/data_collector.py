import pandas as pd
import requests
import logging
from datetime import datetime
import os

logging.basicConfig(
    filename=os.path.join('logs', 'collector.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def fetch_prices():
    
    try:
        response = requests.get("https://api.binance.com/api/v3/ticker/price")
        data = response.json()
        df = pd.DataFrame(data)
        df["timestamp"] = datetime.utcnow()
        logging.info(f"{len(df)} symbols collected from Binance")
        return df
    
    except Exception as e:
          logging.error(f"data extraction error {e}")  
          raise
          
def save_to_csv(path, df):
    try:
        df.to_csv(path, index=False)
        logging.info(f"Data saved successfully in {path}")
    except Exception as e:
        logging.error(f"Error when saving data {e}")
        raise
    
if __name__ == '__main__':
    os.makedirs('data/raw', exist_ok=False)
    df = fetch_prices()
    save_to_csv("data/raw/raw_crypto_prices.csv", df)