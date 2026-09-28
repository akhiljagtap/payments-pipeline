from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.streaming.validation import add_validation_columns

spark = SparkSession.builder.appName("SilverBatchTest").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze_df = spark.read.format("delta").load("/opt/app/data/bronze/transactions")
print("Bronze total:", bronze_df.count())

validated_df = add_validation_columns(bronze_df)

quarantine_df = validated_df.filter(col("rejection_reason").isNotNull())
valid_df = validated_df.filter(col("rejection_reason").isNull())

print("Quarantined rows:", quarantine_df.count())
print("Valid rows (before dedup):", valid_df.count())

print("=== Quarantine breakdown by reason ===")
quarantine_df.groupBy("rejection_reason").count().show()

deduped_df = valid_df.dropDuplicates(["transaction_id"])

print("Valid rows after dedup (final Silver count):", deduped_df.count())
print("Duplicates removed:", valid_df.count() - deduped_df.count())

spark.stop()