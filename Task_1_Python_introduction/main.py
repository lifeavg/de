import logging
from datetime import datetime
from pathlib import Path

import cli
import psycopg as pg
from config import Config

LOG_FORMAT = "[%(asctime)s][%(levelname)s]: %(message)s"
logging.basicConfig(
    format=LOG_FORMAT, filename=f"{datetime.now().timestamp()}.log", filemode="w"
)
logger = logging.getLogger()


def main():
    main_config = Config(Path("config.json"))
    main_config.load()
    if not main_config.connection_address:
        logger.error(
            "Missing configuration data to create db connection %s", main_config
        )
        print("Execution finished, for details see: ", logger.handlers)
        exit(1)
    with pg.connect(  # pylint: disable=E1129
        main_config.connection_address, autocommit=True
    ) as conn:
        cli.Cli(conn).run()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # pylint: disable=W0718
        logger.exception(error)
    print("Execution finished, for details see: ", logger.handlers)
