from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("CryptoETL").getOrCreate()

df = spark.read.csv("raw_crypto_prices.csv", header=True, inferSchema=True)

df = df.withColumn("price", col("price").cast("float"))

windowSpec = Window.partitionBy("symbol").orderBy("timestamp")

df = df.withColumn("moving_avg", avg("price").over(windowSpec.rowsBetween(-4, 0)))

df = df.withColumn("previous_price", lag("price").over(windowSpec))

df = df.withColumn("price_change_pct", ((col("price") - col("previous_price")) / col("previous_price")) * 100)

df.write.mode("overwrite").parquet("processed_crypto_data.parquet")
# df.write.mode("overwrite").csv("processed_crypto_data.csv", header=True)
