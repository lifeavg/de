import json
from pathlib import Path

from util import object_attrs


class Config:
    def __init__(self, file: Path) -> None:
        self.file: Path = file
        self.username: str | None = None
        self.password: str | None = None
        self.host: str | None = None
        self.port: int | None = None
        self.db_name: str | None = None

    def load(self) -> None:
        try:
            with open(self.file, "rb") as file:
                raw = json.load(file)
                self.username = raw.get("username")
                self.password = raw.get("password")
                self.host = raw.get("host")
                self.port = raw.get("port")
                self.db_name = raw.get("db_name")
        except (FileNotFoundError, PermissionError) as error:
            print(self.file, error.args[1])

    @property
    def connection_address(self) -> str:
        if self.username and self.password and self.host and self.port and self.db_name:
            return (
                f"postgresql://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.db_name}"
            )
        return ""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(object_attrs(self))})"
