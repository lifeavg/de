from typing import Any, LiteralString

import psycopg as pg
import sql
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier


class DBInit:
    def __init__(self, connection: pg.Connection) -> None:
        self._connection = connection

    def _create_tables(self) -> None:
        with self._connection.cursor() as cursor, self._connection.transaction():
            cursor.execute(sql.SQL_ROOMS).execute(sql.SQL_STUDENTS)

    def _check_type_exists(self, typename: str) -> bool:
        with self._connection.cursor() as cursor:
            cursor.execute(sql.SQL_CHECK_TYPE, (typename,))
            self._connection.rollback()
            return cursor.rowcount > 0

    def _create_type(self, typename: str) -> None:
        with self._connection.cursor() as cursor, self._connection.transaction():
            cursor.execute(SQL(sql.SQL_SEX_TYPE).format(Identifier(typename)))

    def init(self) -> None:
        if not self._check_type_exists("sex_t"):
            self._create_type("sex_t")
        self._create_tables()


def db_query(connection: pg.Connection, query: LiteralString) -> list[dict[str, Any]]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query)
        connection.rollback()
        return cursor.fetchall()
