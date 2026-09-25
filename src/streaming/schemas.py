from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

payment_event_schema = StructType([
    StructField("schema_version", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", StringType(), True),
])