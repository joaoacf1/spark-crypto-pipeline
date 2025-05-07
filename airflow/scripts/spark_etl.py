from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag, to_date
from pyspark.sql.window import Window
import logging
import os
import glob

os.makedirs(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs'), exist_ok=True)

logging.basicConfig(
    filename=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'etl.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def start_spark():
    return SparkSession.builder.appName("CryptoETL").getOrCreate()
    
def process_data(spark, input_dir, output_path):
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    if not csv_files:
        logging.error(f"No CSV files found in {input_dir}")
        raise FileNotFoundError(f"No CSV files found in {input_dir}")

    df = spark.read.csv(csv_files, header=True, inferSchema=True)

    df = df.withColumn("price", col("price").cast("float"))

    windowSpec = Window.partitionBy("symbol").orderBy("timestamp")

    df = df.withColumn("moving_avg", avg("price").over(windowSpec.rowsBetween(-4, 0)))

    df = df.withColumn("previous_price", lag("price").over(windowSpec))

    df = df.withColumn("price_change_pct", ((col("price") - col("previous_price")) / col("previous_price")) * 100)

    df = df.withColumn("date", to_date("timestamp"))
    
    df.write.mode("append").partitionBy("date").parquet(output_path)
    
    logging.info(f"Data saved successfully in {output_path}")

if __name__ == '__main__':
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        raw_path = os.path.join(base_dir, "data", "raw")
        processed_dir = os.path.join(base_dir, "data", "processed")
        processed_path = os.path.join(processed_dir, "processed_crypto_data.parquet")
        os.makedirs(processed_dir, exist_ok=True)
        spark = start_spark()
        process_data(spark, raw_path, processed_path)
        spark.stop()
    except Exception as e:
        logging.error(f"ETL failed: {e}", exc_info=True)
        import sys
        sys.exit(1)
