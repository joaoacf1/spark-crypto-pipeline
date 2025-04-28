from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CryptoETL").getOrCreate()

df = spark.read.parquet("processed_crypto_data.parquet")

df.createOrReplaceTempView("crypto")

result = spark.sql(
    """
    SELECT symbol,
              price,
              timestamp,
              moving_avg,
              price_change_pct
        FROM crypto
        ORDER BY price_change_pct DESC
        LIMIT 10
              """
)

result.show()