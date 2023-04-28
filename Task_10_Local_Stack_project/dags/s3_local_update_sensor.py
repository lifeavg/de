import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator

logger = logging.getLogger()


class S3LocalFileSyncSensor(BaseSensorOperator):
    def __init__(self, file: Path, bucket_name: str, **kwargs):
        self.file = file
        self.bucket_name = bucket_name
        self._s3_hook: S3Hook | None = None
        super().__init__(**kwargs)

    @property
    def s3_hook(self) -> S3Hook:
        if not self._s3_hook:
            self._s3_hook = S3Hook()  # TODO: non-default s3 hook
        return self._s3_hook

    @property
    def local_last_modified(self) -> datetime:
        return datetime.fromtimestamp(self.file.stat().st_mtime, tz=timezone.utc)

    def load_meta(self) -> dict[str, Any] | None:
        return self.s3_hook.head_object(key=self.file.name, bucket_name=self.bucket_name)  # TODO: custom file key

    def poke(self, context) -> bool:
        meta = self.load_meta()
        # if key not exists
        if not meta:
            logger.info("New file: %s", self.file.name)
            return True
        # if file was modified after last upload
        if meta["LastModified"] < self.local_last_modified:
            logger.info("Update file: %s", self.file.name)
            return True
        logger.info("Skip file: %s", self.file.name)
        return False
