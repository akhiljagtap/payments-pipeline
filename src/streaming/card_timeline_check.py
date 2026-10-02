from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CardTimelineCheck").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/app/data/silver/transactions")

card = df.filter(col("card_id") == "card_00022").orderBy("kafka_timestamp")
card.select("transaction_id", "kafka_timestamp").show(30, truncate=False)

spark.stop()