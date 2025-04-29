from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag
from pyspark.sql.window import Window
import logging
import os

# Cria a pasta de logs se não existir
os.makedirs(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs'), exist_ok=True)

logging.basicConfig(
    filename=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'etl.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def start_spark():
    return SparkSession.builder.appName("CryptoETL").getOrCreate()
    
def process_data(spark, input_path, output_path):
    try:
        if not os.path.exists(input_path):
            logging.error(f"Input file not found: {input_path}")
            raise FileNotFoundError(f"Input file not found: {input_path}")

        df = spark.read.csv(input_path, header=True, inferSchema=True)

        df = df.withColumn("price", col("price").cast("float"))

        windowSpec = Window.partitionBy("symbol").orderBy("timestamp")

        df = df.withColumn("moving_avg", avg("price").over(windowSpec.rowsBetween(-4, 0)))

        df = df.withColumn("previous_price", lag("price").over(windowSpec))

        df = df.withColumn("price_change_pct", ((col("price") - col("previous_price")) / col("previous_price")) * 100)

        df.write.mode("append").parquet(output_path)
        
        logging.info(f"Data saved successfully in {output_path}")

    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

if __name__ == '__main__':
    # Descobre a pasta base (raiz do projeto)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    raw_path = os.path.join(base_dir, "data", "raw", "raw_crypto_prices.csv")
    processed_dir = os.path.join(base_dir, "data", "processed")
    processed_path = os.path.join(processed_dir, "processed_crypto_data.parquet")

    # Cria a pasta de dados processados se não existir
    os.makedirs(processed_dir, exist_ok=True)

    spark = start_spark()
    process_data(spark, raw_path, processed_path)
