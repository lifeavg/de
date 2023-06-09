# spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 --py-files schema.py src/bundle_id_fatal_error_tracker.py
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from schema import schema

spark = SparkSession.builder.appName("bundle_id_fatal_error_tracker").getOrCreate()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

errorsStream = spark.readStream.schema(schema).csv(
    "file:////data",
    header=True,
    multiLine=True,
    escape='"',
    timestampFormat="MM/dd/yyyy HH:mm:ss",
)

errorsStream = (
    errorsStream.withColumn("date", errorsStream["date"].cast(TimestampType()))
    .filter(errorsStream["severity"] == "Error")
    .groupBy(
        errorsStream["bundle_id"],
        f.window("date", "1 hour").alias("timeRange"),
    )
    .count()
    .withColumn(
        "value",
        f.to_json(
            f.struct(
                f.lit("bundle_id_fatal_error").alias("type"),
                f.col("timeRange")["start"].alias("start"),
                f.col("timeRange")["end"].alias("end"),
                f.col("count").alias("errorCount"),
            )
        ),
    )
)
print(errorsStream)

# fmt: off
errorsStream.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "/tmp/test-kafka/checkpoint") \
    .option("topic", "log-alerts") \
    .outputMode("update") \
    .start() \
    .awaitTermination()
# fmt: on
