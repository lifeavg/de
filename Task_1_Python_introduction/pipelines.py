import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Iterable, LiteralString

import psycopg as pg

logger = logging.getLogger()

Reason = str


class JSONPipe(ABC):
    def __init__(self, insert_query: LiteralString) -> None:
        super().__init__()
        self.insert_query: LiteralString = insert_query

    def extract(self, raw_file: Path) -> list[dict[str, Any]]:
        try:
            with open(raw_file, "rb") as file:
                raw_json = json.load(file)
                logger.info("%s, data successfully loaded", raw_file)
                return raw_json
        except (FileNotFoundError, PermissionError) as error:
            logger.error("Unable to read data %s, %s", raw_file, error.args[1])
        return []

    @abstractmethod
    def is_valid(self, item: dict[str, Any]) -> tuple[bool, Reason]:
        ...

    def filter(
        self, items: Iterable[dict[str, Any]]
    ) -> Generator[dict[str, Any], None, None]:
        for item in items:
            valid, reason = self.is_valid(item)
            if valid:
                yield item
            else:
                logger.warning("Invalid input %s: %s", reason, item)

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
                    logger.info("Item successfully saved to DB: %s", item)
                except (
                    pg.errors.UniqueViolation,
                    pg.errors.NumericValueOutOfRange,
                    pg.errors.ForeignKeyViolation,
                    pg.errors.ProgrammingError,
                ) as error:
                    logger.error(
                        "Error on saving item to DB: %s , %s", item, error.args[0]
                    )

    def execute(self, connection: pg.Connection, raw_file: Path) -> None:
        logger.info("Starting pipeline execution for file %s", raw_file)
        self.load(
            connection,
            self.insert_query,
            self.transform(self.filter(self.extract(raw_file))),
        )
        logger.info("Finished pipeline execution for file %s", raw_file)


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
        self, items: Iterable[dict[str, Any]]
    ) -> Generator[dict[str, str | int | datetime], None, None]:
        for item in items:
            try:
                item["birthday"] = datetime.fromisoformat(item["birthday"]).date()
            except ValueError:
                logger.error("Incorrect date format in birthday field: %s", item)
            else:
                yield item
