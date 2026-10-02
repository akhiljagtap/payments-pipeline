from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadGoldFraudVelocity").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/app/data/gold/fraud_velocity_alerts")

print("Total fraud velocity alert rows:", df.count())
df.orderBy("window_start", "card_id").show(50, truncate=False)

spark.stop()