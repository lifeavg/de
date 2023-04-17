from pathlib import Path

import pendulum
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

FILE_DIR = Path("/home/lifeavg/Documents/projects/de/Task_8_Snowflake/data")
FILE = FILE_DIR / Path("763K_plus_IOS_Apps_Info.csv")


class IosAppsInfoSnowflakeOperator(SnowflakeOperator):
    def __init__(
        self,
        *,
        role: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            snowflake_conn_id="test_snowflake",
            warehouse="TEST_WH",
            database="TEST_DB",
            role=role,
            schema="TEST_SH",
            authenticator=authenticator,
            session_parameters=session_parameters,
            **kwargs,
        )


@dag(
    start_date=pendulum.datetime(2023, 4, 17, 7, 35, tz="UTC"),
    schedule_interval="@once",
    tags=["snowflake", "ios", "ratings", "info"],
    description="load ios apps info to snowflake",
)
def ios_apps_info():
    """
    Assuming file format, tables, streams, tasks are created before
    """
    stage_file = IosAppsInfoSnowflakeOperator(task_id="stage_file", sql=f"PUT file://{FILE} @~/staged")

    load_to_raw = IosAppsInfoSnowflakeOperator(
        task_id="load_to_raw",
        sql=f"""COPY INTO TEST_DB.TEST_SH.RAW_TABLE
                FROM @~/staged
                PATTERN = '.*{FILE.name}.*'
                FILE_FORMAT = (FORMAT_NAME = TEST_DB.TEST_SH.IOS_APPS_INFO_FILE)""",
    )

    stage_file >> load_to_raw  # pylint: disable=W0104


ios_apps_info()
