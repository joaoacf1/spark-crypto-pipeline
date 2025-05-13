from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag, to_date, expr
from pyspark.sql.window import Window
import logging
import os

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.makedirs(os.path.join(base_dir, 'logs'), exist_ok=True)
logging.basicConfig(
    filename=os.path.join(base_dir, 'logs', 'etl.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def start_spark():
    spark = SparkSession.builder \
        .appName("CryptoETL") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .getOrCreate()
    return spark
    
def process_data(spark):
    try:
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        if not bucket_name:
            logging.error("S3_BUCKET_NAME environment variable is not set")
            raise ValueError("S3_BUCKET_NAME environment variable is not set")
        
        s3_input_path = f"s3a://{bucket_name}/raw/"
        s3_output_path = f"s3a://{bucket_name}/processed/crypto_data.parquet"
        
        logging.info(f"Reading data from: {s3_input_path}")
        
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("mode", "PERMISSIVE") \
            .schema("""
                symbol STRING,
                price STRING,
                timestamp TIMESTAMP
            """) \
            .csv(s3_input_path)
        
        if df.rdd.isEmpty():
            logging.warning("No data found to process")
            return
            
        logging.info(f"Processing {df.count()} records")
        logging.info("Schema:")
        df.printSchema()
        
        df = df.withColumn("currency", expr("substring(symbol, length(symbol)-3, 4)")).filter(col('currency') == 'USDT').drop('currency')

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

        logging.info(f"Writing processed data to: {s3_output_path}")
        df.write.mode("overwrite").partitionBy("date").parquet(s3_output_path)
        logging.info("Data successfully processed and written to S3")

    except Exception as e:
        logging.error(f"Error processing data: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    try:
        spark = start_spark()
        process_data(spark)
        spark.stop()
        
    except Exception as e:
        logging.error(f"ETL failed: {str(e)}", exc_info=True)
        import sys
        sys.exit(1)
