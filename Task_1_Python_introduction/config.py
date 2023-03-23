import json
from pathlib import Path


class Config:
    def __init__(self, file: Path) -> None:
        self.file: Path = file
        self.username: str | None = None
        self.password: str | None = None
        self.host: str | None = None
        self.port: int | None = None
        self.db_name: str | None = None

    def load(self) -> None:
        with open(self.file, "rb") as file:
            raw = json.load(file)
            self.username = raw.get("username")
            self.password = raw.get("password")
            self.host = raw.get("host")
            self.port = raw.get("port")
            self.db_name = raw.get("db_name")

    @property
    def connection_address(self) -> str:
        if self.username and self.password and self.host and self.port and self.db_name:
            return (
                f"postgresql://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.db_name}"
            )
        return ""