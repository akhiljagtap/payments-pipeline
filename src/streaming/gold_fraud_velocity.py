from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count

spark = SparkSession.builder.appName("GoldFraudVelocity").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver_stream = (
    spark.readStream
    .format("delta")
    .load("/opt/app/data/silver/transactions")
    .withWatermark("kafka_timestamp", "10 minutes")
)

velocity_alerts = (
    silver_stream
    .groupBy(
        window(col("kafka_timestamp"), "5 minutes", "1 minute"),
        col("card_id")
    )
    .agg(count("*").alias("txn_count_in_window"))
    .filter(col("txn_count_in_window") >= 5)
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("card_id"),
        col("txn_count_in_window"),
    )
)

query = (
    velocity_alerts.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/app/checkpoints/gold_fraud_velocity")
    .trigger(processingTime="60 seconds")
    .start("/opt/app/data/gold/fraud_velocity_alerts")
)
query.awaitTermination()