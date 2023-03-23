import json
from pathlib import Path
from typing import Any

import dicttoxml


def to_json(data: list[dict[str, Any]]) -> str:
    return json.dumps(data)


def to_xml(data: list[dict[str, Any]]) -> str:
    return dicttoxml.dicttoxml(data, return_bytes=False, attr_type=False)  # type: ignore


def write(data: str, out_file: Path) -> None:
    try:
        with open(out_file, "w", encoding="utf-8") as file:
            file.write(data)
    except PermissionError as error:
        print(out_file, error.args[1])
