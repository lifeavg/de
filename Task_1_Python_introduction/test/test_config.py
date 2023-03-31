from pathlib import Path

from config import Config


def test_connection_address_ok():
    config = Config(Path())
    config.username = "username"
    config.password = "password"
    config.host = "host"
    config.port = 1
    config.db_name = "db_name"
    assert config.connection_address == "postgresql://username:password@host:1/db_name"


def test_connection_address_missing_param():
    config = Config(Path())
    assert config.connection_address == ""
