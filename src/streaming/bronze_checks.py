from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("BronzeChecks").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/app/data/bronze/transactions")

print("Total rows:", df.count())

print("=== Duplicate transaction_ids (appear more than once) ===")
dup_df = df.groupBy("transaction_id").count().filter(col("count") > 1)
print("Number of distinct duplicated transaction_ids:", dup_df.count())
extra = dup_df.selectExpr("sum(count - 1) as extra").collect()[0]["extra"]
print("Total extra duplicate rows (count - 1 summed):", extra)
dup_df.show(truncate=False)

print("=== Rows with null card_id (corrupt: missing_field) ===")
null_card_df = df.filter(col("card_id").isNull())
print("Count:", null_card_df.count())
null_card_df.select("transaction_id", "raw_json").show(truncate=False)

print("=== Rows where amount is negative (corrupt: negative_amount) ===")
neg_amount_df = df.filter(col("amount") < 0)
print("Count:", neg_amount_df.count())
neg_amount_df.select("transaction_id", "amount").show(truncate=False)

print("=== Rows with unknown currency (corrupt: bad_currency) ===")
bad_currency_df = df.filter(~col("currency").isin("INR"))
print("Count:", bad_currency_df.count())
bad_currency_df.select("transaction_id", "currency").show(truncate=False)

print("=== Rows with completely broken JSON (all fields null) ===")
broken_json_df = df.filter(col("transaction_id").isNull())
print("Count:", broken_json_df.count())
broken_json_df.select("raw_json").show(truncate=False)

spark.stop() 