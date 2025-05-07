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
    spark = SparkSession.builder \
        .appName("CryptoETL") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .getOrCreate()
    return spark
    
def process_data(spark, input_dir, output_path):
    abs_input_dir = os.path.abspath(input_dir)
    abs_output_path = os.path.abspath(output_path)
    logging.info(f"Input directory (absolute): {abs_input_dir}")
    logging.info(f"Output path (absolute): {abs_output_path}")
    
    csv_files = glob.glob(os.path.join(abs_input_dir, "*.csv"))
    logging.info(f"CSV files found: {csv_files}")
    
    if not csv_files:
        logging.error(f"No CSV files found in {abs_input_dir}")
        raise FileNotFoundError(f"No CSV files found in {abs_input_dir}")

    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("mode", "PERMISSIVE") \
            .schema("""
                symbol STRING,
                price STRING,
                timestamp TIMESTAMP
            """) \
            .csv(csv_files)
        
        logging.info("Successfully read CSV files")
        logging.info("Schema:")
        df.printSchema()
        logging.info("Sample data:")
        df.show(5, truncate=False)

        df = df.withColumn("price_float", 
            col("price").cast("float")
        ).drop("price").withColumnRenamed("price_float", "price")

        windowSpec = Window.partitionBy("symbol").orderBy("timestamp")

        df = df.withColumn("moving_avg", avg("price").over(windowSpec.rowsBetween(-4, 0)))
        df = df.withColumn("previous_price", lag("price").over(windowSpec))
        df = df.withColumn("price_change_pct", 
            ((col("price") - col("previous_price")) / col("previous_price")) * 100
        )
        df = df.withColumn("date", to_date("timestamp"))

        logging.info(f"Writing data to: {abs_output_path}")
        df.write.mode("append").partitionBy("date").parquet(abs_output_path)
        logging.info("Data successfully written")

    except Exception as e:
        logging.error(f"Error processing data: {str(e)}", exc_info=True)
        raise

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
        logging.error(f"ETL failed: {str(e)}", exc_info=True)
        import sys
        sys.exit(1)
