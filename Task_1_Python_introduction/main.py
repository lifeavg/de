from pathlib import Path

import cli
import psycopg as pg
from config import Config


def main():
    main_config = Config(Path("config.json"))
    main_config.load()
    if not main_config.connection_address:
        print(f"Missing configuration data to create db connection {main_config}")
        exit(1)
    with pg.connect(  # pylint: disable=E1129
        main_config.connection_address, autocommit=True
    ) as conn:
        cli.Cli(conn).run()


if __name__ == "__main__":
    main()
