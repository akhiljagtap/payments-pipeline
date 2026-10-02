from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("SilverCardCheck").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/app/data/silver/transactions")

print("Total Silver rows:", df.count())
print("Distinct card_ids used:", df.select("card_id").distinct().count())

print("=== Top 10 cards by transaction count ===")
df.groupBy("card_id").agg(count("*").alias("txn_count")).orderBy(col("txn_count").desc()).show(10, truncate=False)

spark.stop()