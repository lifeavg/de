import logging
from datetime import datetime, timezone
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

logger = logging.getLogger()

FILE_DIR = Path("/home/lifeavg/Documents/projects/de/Task_10_Local_Stack_project/data")
BUCKET = 'helsinki-city-bikes'


@task()
def sync_files(bucket_name: str) -> None:
    s3 = S3Hook()
    # assuming folder has only files
    for file in FILE_DIR.iterdir():
        meta = s3.head_object(key=file.name, bucket_name=bucket_name)
        # if key not exists
        if not meta:
            s3.load_file(filename=file, key=file.name, bucket_name=bucket_name)
            logger.info("Uploaded new file: %s", file.name)
            continue
        local_modification_date = datetime.fromtimestamp(file.stat().st_mtime, tz=timezone.utc)
        # if file was modified after last upload
        if meta['LastModified'] < local_modification_date:
            s3.load_file(filename=file, key=file.name, replace=True, bucket_name=bucket_name)
            logger.info("Updated file: %s", file.name)
            continue
        logger.info("Skip file: %s", file.name)


@dag(
    start_date=pendulum.datetime(2023, 4, 27, 13, 40, tz="UTC"),
    schedule_interval='@once',
    tags=["files", "s3", "reviews"],
    description="load files to s3, duplicates are replaced"
)
def load_to_s3():
    # should log 'already exists' or create
    create_bucket = S3CreateBucketOperator(
        task_id='create_bucket',
        bucket_name=BUCKET
    )
    create_bucket >> sync_files(BUCKET)


load_to_s3()
