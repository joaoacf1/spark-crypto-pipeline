from pyspark.sql import SparkSession
import logging
import os

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

os.makedirs(os.path.join(base_dir, 'logs'), exist_ok=True)

logging.basicConfig(
    filename=os.path.join(os.path.join(base_dir, 'logs'), 'queries.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def run_query():

      try:
            spark = SparkSession.builder.appName("CryptoQueries").getOrCreate()

            df = spark.read.parquet(os.path.join(base_dir, "data", "processed", "processed_crypto_data.parquet"))

            df.createOrReplaceTempView("crypto")

            result = spark.sql(
            """
            SELECT symbol,
                   AVG(price) AS avg_price,
                   MAX(price_change_pct) AS max_change,
                   MIN(price_change_pct) AS min_change
            FROM crypto
            GROUP BY symbol
            ORDER BY avg_price DESC
            LIMIT 10
                        """
            )

            result.show()
            
            logging.info("Data query successfully")
            
      except Exception as e:
            logging.error(f"Error when querying data {e}")
            raise
      

if __name__ == "__main__":
      run_query()
      
      