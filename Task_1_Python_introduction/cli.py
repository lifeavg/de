import argparse
import sys
from pathlib import Path

import db
import exporters as exp
import pipelines as ppl
import sql
from psycopg import Connection


class Cli:  # pylint: disable=R0903
    def __init__(self, connection: Connection) -> None:
        self._reports = {
            "SQL_ROOMS_STUD_NUM": sql.SQL_ROOMS_STUD_NUM,
            "SQL_AVG_YOUNGEST_ROOMS": sql.SQL_AVG_YOUNGEST_ROOMS,
            "SQL_BIGGEST_AGE_DIFF": sql.SQL_BIGGEST_AGE_DIFF,
            "SQL_ROOMS_DIFF_SEX": sql.SQL_ROOMS_DIFF_SEX,
        }
        self._exporters = {
            "json": exp.to_json,
            "xml": exp.to_xml,
        }
        self._connection = connection

        # root argument parser
        self.arguments = argparse.ArgumentParser(prog="dormitory")
        subparsers = self.arguments.add_subparsers()
        # subparser for database init script
        init = subparsers.add_parser("init")
        init.set_defaults(command="init")
        # subparser for file ETL pipelines
        extract = subparsers.add_parser("extract")
        extract.set_defaults(command="extract")
        extract.add_argument(
            "-students",
            metavar="F",
            type=Path,
            nargs="?",
            help="JSON file with students records",
        )
        extract.add_argument(
            "-rooms",
            metavar="F",
            type=Path,
            nargs="?",
            help="JSON file with rooms records",
        )
        # subparser for reporting
        report = subparsers.add_parser("report")
        report.set_defaults(command="report")
        report.add_argument(
            "report_type",
            metavar="S",
            type=str,
            nargs=1,
            choices=tuple(self._reports.keys()),
            help=f"Report to create: {tuple(self._reports.keys())}",
        )
        report.add_argument("file", metavar="F", type=Path, nargs=1, help="Output file path")
        report.add_argument(
            "-format",
            metavar="T",
            default="json",
            choices=tuple(self._exporters.keys()),
            type=str,
            nargs="?",
            help=f"Output format: {tuple(self._exporters.keys())}",
        )

    def run(self, raw_args: list[str] | None = None) -> None:
        if len(sys.argv) < 2:
            print("Please, input valid command: extract, report, init")
            return
        args = self.arguments.parse_args(raw_args)
        match args.command:
            case "extract":
                if args.rooms:
                    ppl.RoomPipe(sql.SQL_INSERT_ROOM).execute(self._connection, args.rooms)
                if args.students:
                    ppl.RoomPipe(sql.SQL_INSERT_STUDENT).execute(self._connection, args.students)
            case "report":
                exp.write(
                    self._exporters[args.format](db.db_query(self._connection, self._reports[args.report_type[0]])),  # type: ignore
                    args.file[0],
                )
            case "init":
                db.DBInit(self._connection).init()
