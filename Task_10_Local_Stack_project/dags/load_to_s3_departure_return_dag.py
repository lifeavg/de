import logging

import pendulum
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
)
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config import Config
from s3_local_update_sensor import S3LocalFileSyncSensor

logger = logging.getLogger()


@dag(
    start_date=pendulum.datetime(2023, 4, 27, 13, 40, tz="UTC"),
    schedule="@once",
    tags=["files", "s3", "reviews"],
    description="load files to s3, duplicates are replaced",
)
def load_to_s3_departure_return():
    # should log 'already exists' or create
    create_bucket = S3CreateBucketOperator(task_id="create_bucket", bucket_name=Config().bucket)

    check_sync = S3LocalFileSyncSensor(
        task_id="check_sync",
        file=Config().file,
        bucket_name=Config().bucket,
        soft_fail=True,
        poke_interval=3,
        timeout=10,
    )

    load_file = S3CreateObjectOperator(
        task_id="load_file",
        s3_bucket=Config().bucket,
        s3_key=Config().file.name,
        data=Config().file.read_bytes(),
        replace=True,
    )

    calculate_metrics = SparkSubmitOperator(
        task_id="calculate_metrics",
        application="${SPARK_HOME}/jobs/bikes/helsinki/calculate_metrics.py",
    )

    create_bucket >> check_sync >> [load_file, calculate_metrics]  # type: ignore


load_to_s3_departure_return()
