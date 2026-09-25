from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from src.streaming.schemas import payment_event_schema

spark = SparkSession.builder.appName("BronzeIngestion").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "payments-kafka:9092")
    .option("subscribe", "payments.transactions")
    .option("startingOffsets", "earliest")
    .load()
)

bronze_df = raw_stream.select(
    col("value").cast("string").alias("raw_json"),
    from_json(col("value").cast("string"), payment_event_schema).alias("parsed"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingested_at"),
)

bronze_df = bronze_df.select(
    "parsed.*",
    "raw_json",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
)

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/app/checkpoints/bronze")
    .start("/opt/app/data/bronze/transactions")
)

query.awaitTermination()