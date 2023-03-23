import logging
from typing import Any, LiteralString

import psycopg as pg
import sql
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier

logger = logging.getLogger()


class DBInit:  # pylint: disable=R0903
    def __init__(self, connection: pg.Connection) -> None:
        self._connection = connection

    def _create_tables(self) -> None:
        with self._connection.cursor() as cursor, self._connection.transaction():
            cursor.execute(sql.SQL_ROOMS).execute(sql.SQL_STUDENTS)
        logger.info("Successfully created tables")

    def _check_type_exists(self, typename: str) -> bool:
        with self._connection.cursor() as cursor:
            cursor.execute(sql.SQL_CHECK_TYPE, (typename,))
            self._connection.rollback()
            return cursor.rowcount > 0

    def _create_type(self, typename: str) -> None:
        with self._connection.cursor() as cursor, self._connection.transaction():
            cursor.execute(SQL(sql.SQL_SEX_TYPE).format(Identifier(typename)))
        logger.info("Successfully created type: %s", typename)

    def init(self) -> None:
        """
        execute DB initialization.
        creates requeued types and warns if type_name exists (no overwrite).
        creates tables assuming SQL script has "if not exists" check
        """
        typename = "sex_t"
        if not self._check_type_exists(typename):
            self._create_type(typename)
        else:
            logger.warning("Type %s already exists", typename)
        self._create_tables()
        logger.info("DB initialized")


def db_query(connection: pg.Connection, query: LiteralString) -> list[dict[str, Any]]:
    logger.info("Running query %s", query)
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query)
        connection.rollback()
        query_data = cursor.fetchall()
        logger.info("Query successfully executed %s", query)
        return query_data
