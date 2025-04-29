from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag
from pyspark.sql.window import Window
import logging
import os

logging.basicConfig(
    filename=os.path.join('logs', 'etl.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def start_spark():
    return SparkSession.builder.appName("CryptoETL").getOrCreate()
    
def process_data(spark, input_path, output_path):
    
    try:
    
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        df = df.withColumn("price", col("price").cast("float"))

        windowSpec = Window.partitionBy("symbol").orderBy("timestamp")

        df = df.withColumn("moving_avg", avg("price").over(windowSpec.rowsBetween(-4, 0)))

        df = df.withColumn("previous_price", lag("price").over(windowSpec))

        df = df.withColumn("price_change_pct", ((col("price") - col("previous_price")) / col("previous_price")) * 100)

        df.write.mode("overwrite").parquet(output_path)
        
        logging.info(f"Data saved in {output_path}")

    except Exception as e:
        logging.error(f"Error when processed data {e}")
        raise
    
if __name__ == '__main__':
    os.makedirs("data/processed", exist_ok=True)
    spark = start_spark()
    process_data(spark, "data/raw/raw_crypto_prices.csv", "data/processed/processed_crypto_data.parquet")