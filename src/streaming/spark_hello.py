from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HelloSpark").master("local[*]").getOrCreate()

data = [("card_00001", 100.0), ("card_00002", 250.5)]
df = spark.createDataFrame(data, ["card_id", "amount"])

df.show()
print("Row count:", df.count())

spark.stop()