import logging
from datetime import datetime, timezone

import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from config import Config

logger = logging.getLogger()


@task()
def sync_files(bucket_name: str) -> None:
    s3 = S3Hook()
    # assuming folder has only files
    for file in Config().file_dir.iterdir():
        meta = s3.head_object(key=file.name, bucket_name=bucket_name)
        # if key not exists
        if not meta:
            s3.load_file(filename=file, key=file.name, bucket_name=bucket_name)
            logger.info("Uploaded new file: %s", file.name)
            continue
        local_modification_date = datetime.fromtimestamp(
            file.stat().st_mtime, tz=timezone.utc
        )
        # if file was modified after last upload
        if meta["LastModified"] < local_modification_date:
            s3.load_file(
                filename=file, key=file.name, replace=True, bucket_name=bucket_name
            )
            logger.info("Updated file: %s", file.name)
            continue
        logger.info("Skip file: %s", file.name)


@dag(
    start_date=pendulum.datetime(2023, 4, 27, 13, 40, tz="UTC"),
    schedule_interval="@once",
    tags=["files", "s3", "reviews"],
    description="load files to s3, duplicates are replaced",
)
def load_to_s3():
    # should log 'already exists' or create
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket", bucket_name=Config().bucket
    )
    create_bucket >> sync_files(Config().bucket)


load_to_s3()
