from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from src.streaming.validation import add_validation_columns, add_lateness_column

spark = SparkSession.builder.appName("SilverIngestion").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze_stream = (
    spark.readStream
    .format("delta")
    .load("/opt/app/data/bronze/transactions")
    .withWatermark("kafka_timestamp", "10 minutes")
)

processed = add_validation_columns(bronze_stream)
processed = add_lateness_column(processed)

def upsert_to_silver(batch_df, batch_id):
    batch_df = batch_df.filter(col("rejection_reason").isNull())
    batch_df = batch_df.dropDuplicates(["transaction_id"])

    if not DeltaTable.isDeltaTable(spark, "/opt/app/data/silver/transactions"):
        batch_df.write.format("delta").mode("overwrite").save("/opt/app/data/silver/transactions")
        return

    silver_table = DeltaTable.forPath(spark, "/opt/app/data/silver/transactions")
    (
        silver_table.alias("target")
        .merge(batch_df.alias("source"), "target.transaction_id = source.transaction_id")
        .whenNotMatchedInsertAll()
        .execute()
    )

def write_to_quarantine(batch_df, batch_id):
    batch_df = batch_df.filter(col("rejection_reason").isNotNull())
    if batch_df.count() > 0:
        batch_df.write.format("delta").mode("append").save("/opt/app/data/quarantine/transactions")

query_silver = (
    processed.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", "/opt/app/checkpoints/silver")
    .start()
)

query_quarantine = (
    processed.writeStream
    .foreachBatch(write_to_quarantine)
    .option("checkpointLocation", "/opt/app/checkpoints/quarantine")
    .start()
)

spark.streams.awaitAnyTermination()