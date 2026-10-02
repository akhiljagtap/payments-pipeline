from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadGoldMetrics").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/app/data/gold/merchant_metrics")

print("Total window-merchant rows:", df.count())
df.orderBy("window_start", "merchant_id").show(30, truncate=False)

spark.stop()