import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReconciliationCheck").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze_count = spark.read.format("delta").load("/opt/app/data/bronze/transactions").count()
silver_count = spark.read.format("delta").load("/opt/app/data/silver/transactions").count()
quarantine_count = spark.read.format("delta").load("/opt/app/data/quarantine/transactions").count()

# Count duplicate transaction_ids in Bronze: total rows minus distinct transaction_ids
bronze_df = spark.read.format("delta").load("/opt/app/data/bronze/transactions")
distinct_txn_count = bronze_df.select("transaction_id").distinct().count()
duplicate_count = bronze_count - distinct_txn_count

expected_silver = distinct_txn_count - quarantine_count

print(f"Bronze total rows: {bronze_count}")
print(f"Distinct transaction_ids in Bronze: {distinct_txn_count}")
print(f"Duplicates removed: {duplicate_count}")
print(f"Quarantined rows: {quarantine_count}")
print(f"Expected Silver count: {expected_silver}")
print(f"Actual Silver count: {silver_count}")

spark.stop()

if expected_silver != silver_count:
    print("RECONCILIATION FAILED: expected and actual Silver counts do not match!")
    sys.exit(1)
else:
    print("RECONCILIATION PASSED: Silver count matches expected value.")
    sys.exit(0)