from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, sum as _sum, avg, when

spark = SparkSession.builder.appName("GoldDailySummary").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver_stream = (
    spark.readStream
    .format("delta")
    .load("/opt/app/data/silver/transactions")
    .withWatermark("kafka_timestamp", "10 minutes")
)

daily_summary = (
    silver_stream
    .groupBy(
        window(col("kafka_timestamp"), "1 day"),
        col("country"),
        col("channel"),
    )
    .agg(
        count("*").alias("transaction_count"),
        _sum("amount").alias("total_amount"),
        avg(when(col("status") == "APPROVED", 1.0).otherwise(0.0)).alias("approval_rate"),
    )
    .select(
        col("window.start").alias("day_start"),
        col("window.end").alias("day_end"),
        col("country"),
        col("channel"),
        col("transaction_count"),
        col("total_amount"),
        col("approval_rate"),
    )
)

query = (
    daily_summary.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("checkpointLocation", "/opt/app/checkpoints/gold_daily_summary_test")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()