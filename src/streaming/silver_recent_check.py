from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("SilverRecentCheck").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/app/data/silver/transactions")

# Only look at today's run — adjust the date if needed
recent = df.filter(col("kafka_timestamp") >= "2026-10-02 00:00:00")

print("Rows from today:", recent.count())
print("=== Top 10 cards by txn count, TODAY ONLY ===")
recent.groupBy("card_id").agg(count("*").alias("txn_count")).orderBy(col("txn_count").desc()).show(10, truncate=False)

spark.stop()