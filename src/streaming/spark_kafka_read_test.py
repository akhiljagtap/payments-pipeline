#script that connects Spark to our live Kafka topic(payments.transactions)


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("KafkaReadTest").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "payments-kafka:9092")
    .option("subscribe", "payments.transactions")
    .option("startingOffsets", "earliest")
    .load()
)

readable = raw_stream.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("kafka_value"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)

query = (
    readable.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()