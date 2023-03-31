import json
import logging
from pathlib import Path
from typing import Any

import dicttoxml

logger = logging.getLogger()


def to_json(data: list[dict[str, Any]]) -> str:
    return json.dumps(data)


def to_xml(data: list[dict[str, Any]]) -> str:
    return dicttoxml.dicttoxml(data, return_bytes=False, attr_type=False)  # type: ignore


def write(data: str, out_file: Path) -> None:
    """
    writes exported data to file. OVERWRITES file if exists
    """
    try:
        with open(out_file, "w", encoding="utf-8") as file:
            file.write(data)
        logger.info("Report successfully written to: %s", out_file)
    except PermissionError as error:
        logger.info("Unable to write report to: %s, %s", out_file, error.args[1])
