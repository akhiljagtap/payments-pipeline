from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("BronzeChecks").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/app/data/bronze/transactions")

print("=== Duplicate transaction_ids (appear more than once) ===")
df.groupBy("transaction_id").count().filter(col("count") > 1).show(truncate=False)

print("=== Rows with null card_id (corrupt: missing_field) ===")
df.filter(col("card_id").isNull()).select("transaction_id", "raw_json").show(truncate=False)

print("=== Rows where amount is negative (corrupt: negative_amount) ===")
df.filter(col("amount") < 0).select("transaction_id", "amount").show(truncate=False)

spark.stop()