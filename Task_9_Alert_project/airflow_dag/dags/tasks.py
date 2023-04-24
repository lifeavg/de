import json
from typing import List

from airflow.decorators import task
from globals import FILE_DIR
from pendulum.datetime import DateTime


@task(provide_context=True)
def get_file_names(**kwargs) -> str:
    """
    get list of files created in task run time range
    """
    start: DateTime = kwargs["data_interval_start"]
    end: DateTime = kwargs["data_interval_end"]
    files: List[str] = []
    for file in FILE_DIR.iterdir():
        modified = file.stat().st_mtime
        if start.timestamp() <= modified and end.timestamp() > modified:
            files.append(str(file.absolute()))
    return json.dumps(files)
