from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadBronze").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/app/data/bronze/transactions")

print("Total rows:", df.count())
df.show(20, truncate=False)

spark.stop()