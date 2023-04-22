from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

schema = StructType(
    [
        StructField("error_code", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("log_location", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("model", StringType(), True),
        StructField("graphics", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("sdkv", StringType(), True),
        StructField("test_mode", BooleanType(), True),
        StructField("flow_id", StringType(), True),
        StructField("flow_type", StringType(), True),
        StructField("sdk_date", TimestampType(), True),
        StructField("publisher_id", StringType(), True),
        StructField("game_id", StringType(), True),
        StructField("bundle_id", StringType(), True),
        StructField("appv", StringType(), True),
        StructField("language", StringType(), True),
        StructField("os", StringType(), True),
        StructField("adv_id", StringType(), True),
        StructField("gdpr", BooleanType(), True),
        StructField("ccpa", BooleanType(), True),
        StructField("country_code", StringType(), True),
        StructField("date", FloatType(), True),
    ]
)
