from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadGoldDailySummary").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/app/data/gold/daily_summary")

print("Total daily summary rows:", df.count())
df.orderBy("day_start", "country", "channel").show(50, truncate=False)

spark.stop()