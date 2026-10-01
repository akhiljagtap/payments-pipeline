from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql.functions import unix_timestamp, to_timestamp

VALID_CURRENCIES = ["INR"]

def add_validation_columns(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "rejection_reason",
        when(col("card_id").isNull(), "missing_card_id")
        .when(col("merchant_id").isNull(), "missing_merchant_id")
        .when(col("transaction_id").isNull(), "unparseable_json")
        .when(col("amount").isNull(), "missing_amount")
        .when(col("amount") <= 0, "non_positive_amount")
        .when(~col("currency").isin(VALID_CURRENCIES), "invalid_currency")
        .otherwise(lit(None))
    )

def add_lateness_column(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_late",
        (unix_timestamp("kafka_timestamp") - unix_timestamp(to_timestamp("event_time"))) > 120
    )