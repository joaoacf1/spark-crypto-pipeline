import pandas as pd
import requests
from datetime import datetime

def fetch_prices():
    response = requests.get("https://api.binance.com/api/v3/ticker/price")
    data = response.json()
    df = pd.DataFrame(data)
    df["timestamp"] = datetime.utcnow()
    return df
    
    
if __name__ == '__main__':
    df = fetch_prices()
    df.to_csv("raw_crypto_prices.csv", index=False)