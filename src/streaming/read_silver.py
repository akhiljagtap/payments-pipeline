from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadSilver").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver_df = spark.read.format("delta").load("/opt/app/data/silver/transactions")
quarantine_df = spark.read.format("delta").load("/opt/app/data/quarantine/transactions")

print("Silver row count:", silver_df.count())
print("Quarantine row count:", quarantine_df.count())
print("Late rows in Silver:", silver_df.filter("is_late = true").count())

spark.stop()