from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType
)

schema = StructType(
    [
        StructField("departure", TimestampType(), True),
        StructField("return", TimestampType(), True),
        StructField("departure_id", StringType(), True),
        StructField("departure_name", StringType(), True),
        StructField("return_id", StringType(), True),
        StructField("return_name", StringType(), True),
        StructField("distance (m)", FloatType(), True),
        StructField("duration (sec.)", FloatType(), True),
        StructField("avg_speed (km/h)", FloatType(), True),
        StructField("departure_latitude", FloatType(), True),
        StructField("departure_longitude", FloatType(), True),
        StructField("return_latitude", FloatType(), True),
        StructField("return_longitude", FloatType(), True),
        StructField("Air temperature (degC)", FloatType(), True),
    ]
)
