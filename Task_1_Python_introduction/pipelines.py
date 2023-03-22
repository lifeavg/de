import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Iterable, LiteralString

import psycopg as pg

Reason = str


class JSONPipe(ABC):
    def __init__(self, insert_query: LiteralString) -> None:
        super().__init__()
        self.insert_query: LiteralString = insert_query

    def extract(self, raw_file: Path) -> list[dict[str, Any]]:
        with open(raw_file, "rb") as file:
            return json.load(file)

    @abstractmethod
    def is_valid(self, item: dict[str, Any]) -> tuple[bool, Reason]:
        ...

    def filter(
        self, rooms: Iterable[dict[str, Any]]
    ) -> Generator[dict[str, Any], None, None]:
        for room in rooms:
            valid, reason = self.is_valid(room)
            if valid:
                yield room
            else:
                print(reason)

    @abstractmethod
    def transform(
        self, items: Iterable[dict[str, Any]]
    ) -> Generator[dict[str, Any], None, None]:
        ...

    def load(
        self,
        connection: pg.Connection,
        insert_query: LiteralString,
        items: Iterable[dict[str, Any]],
    ) -> None:
        with connection.cursor() as cursor:
            for item in items:
                try:
                    cursor.execute(insert_query, item)
                except (
                    pg.errors.UniqueViolation,
                    pg.errors.NumericValueOutOfRange,
                    pg.errors.ForeignKeyViolation,
                ) as error:
                    print(item, error.args[0])

    def execute(self, connection: pg.Connection, raw_file: Path) -> None:
        self.load(
            connection,
            self.insert_query,
            self.transform(self.filter(self.extract(raw_file))),
        )


class RoomPipe(JSONPipe):
    def transform(
        self, items: Iterable[dict[str, Any]]
    ) -> Generator[dict[str, Any], None, None]:
        for item in items:
            yield item

    def is_valid(self, item: dict[str, Any]) -> tuple[bool, Reason]:
        if not isinstance(item.get("id"), int):
            return False, "invalid id"
        if not isinstance(item.get("name"), str):
            return False, "invalid name"
        return True, "ok"


class StudentPipe(JSONPipe):
    def is_valid(self, item: dict[str, Any]) -> tuple[bool, Reason]:
        if not isinstance(item.get("id"), int):
            return False, "invalid id"
        if not isinstance(item.get("name"), str):
            return False, "invalid name"
        if not isinstance(item.get("room"), int):
            return False, "invalid room"
        if not isinstance(item.get("birthday"), str):
            return False, "invalid birthday"
        sex = item.get("sex")
        if not isinstance(sex, str) or sex not in ("F", "M"):
            return False, "invalid sex"
        return True, "ok"

    def transform(
        self, items: Iterable[dict[str, str | int]]
    ) -> Generator[dict[str, str | int | datetime], None, None]:
        for item in items:
            try:
                item["birthday"] = datetime.fromisoformat(item["birthday"]).date()  # type: ignore
            except ValueError as error:
                print(item, error.args[0])
            else:
                yield item  # type: ignore
