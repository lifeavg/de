import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from schema import schema

packages = [
    f'org.apache.hadoop:hadoop-aws:3.3.1',
    'com.google.guava:guava:30.1.1-jre',
    'org.apache.httpcomponents:httpcore:4.4.14',
    'com.google.inject:guice:4.2.2',
    'com.google.inject.extensions:guice-servlet:4.2.2'
]

spark = (SparkSession.builder
         .appName("helsinki_bikes_metrics")
         .config("spark.jars.packages", ','.join(packages))
         .getOrCreate()
         )

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("com.amazonaws.services.s3a.enableV4", "true")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
hadoop_conf.set("fs.s3a.access.key", "mykey")
hadoop_conf.set("fs.s3a.secret.key", "mykeysecret")
hadoop_conf.set("fs.s3a.session.token", "mock")
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:4566")

data = spark.read.schema(schema).csv(
    '/home/lifeavg/Documents/projects/de/Task_10_Local_Stack_project/data/helsinki_bikes_2016_5.csv',
    header=True
)

departure_count = (data
                   .select("departure_id")
                   .groupBy("departure_id")
                   .agg(f.count("departure_id").alias("departure_count"))
                   )

return_count = (data
                .select("return_id")
                .groupBy("return_id")
                .agg(f.count("return_id").alias("return_count"))
                )

departure_return_count = (
    departure_count.join(
        other=return_count,
        on=(departure_count["departure_id"] == return_count["return_id"]),
        how="fullouter"
    )
    .withColumn("station_id", f.coalesce("departure_id", "return_id"))
    .select("station_id", "departure_count", "return_count")
)

departure_return_count \
    .coalesce(1) \
    .write \
    .csv(
    path="s3a://helsinki-city-bikes/helsinki_bikes_2016_5_metrics",
    mode='overwrite',
    header=True,
    encoding='utf-8'
)
