from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, sum as _sum, avg, when

spark = SparkSession.builder.appName("GoldMerchantMetrics").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver_stream = (
    spark.readStream
    .format("delta")
    .load("/opt/app/data/silver/transactions")
    .withWatermark("kafka_timestamp", "10 minutes")
)

merchant_metrics = (
    silver_stream
    .groupBy(
        window(col("kafka_timestamp"), "5 minutes"),
        col("merchant_id")
    )
    .agg(
        count("*").alias("transaction_count"),
        _sum("amount").alias("total_amount"),
        avg(when(col("status") == "APPROVED", 1.0).otherwise(0.0)).alias("approval_rate")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("merchant_id"),
        col("transaction_count"),
        col("total_amount"),
        col("approval_rate")
    )
)

query = (
    merchant_metrics.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/app/checkpoints/gold_merchant_metrics")
    .trigger(processingTime="30 seconds")
    .start("/opt/app/data/gold/merchant_metrics")
)

query.awaitTermination()