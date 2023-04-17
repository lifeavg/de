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
    Assuming file format, tables and streams are created before
    """
    stage_file = IosAppsInfoSnowflakeOperator(task_id="stage_file", sql=f"PUT file://{FILE} @~/staged")

    load_to_raw = IosAppsInfoSnowflakeOperator(
        task_id="load_to_raw",
        sql=f"""COPY INTO TEST_DB.TEST_SH.RAW_TABLE
                FROM @~/staged
                PATTERN = '.*{FILE.name}.*'
                FILE_FORMAT = (FORMAT_NAME = TEST_DB.TEST_SH.IOS_APPS_INFO_FILE)""",
    )

    stream_to_stage = IosAppsInfoSnowflakeOperator(
        task_id="stream_to_stage",
        sql="""MERGE INTO TEST_DB.TEST_SH.STAGE_TABLE ST
                USING TEST_DB.TEST_SH.RAW_STREAM RS
                ON ST.id = RS.id
                WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE'
                    THEN DELETE
                WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'
                    THEN UPDATE SET ST.id = RS.id,
                                    ST.ios_app_id = RS.ios_app_id,
                                    ST.title = RS.title,
                                    ST.developer_name = RS.developer_name,
                                    ST.developer_ios_id = RS.developer_ios_id,
                                    ST.ios_store_url = RS.ios_store_url,
                                    ST.seller_official_website = RS.seller_official_website,
                                    ST.age_rating = RS.age_rating,
                                    ST.total_average_rating = RS.total_average_rating,
                                    ST.total_number_of_ratings = RS.total_number_of_ratings,
                                    ST.average_rating_for_version = RS.average_rating_for_version,
                                    ST.number_of_ratings_for_version = RS.number_of_ratings_for_version,
                                    ST.original_release_date = RS.original_release_date,
                                    ST.current_version_release_date = RS.current_version_release_date,
                                    ST.price_usd = RS.price_usd,
                                    ST.primary_genre = RS.primary_genre,
                                    ST.all_genres = RS.all_genres,
                                    ST.languages = RS.languages,
                                    ST.description = RS.description
                WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
                    THEN INSERT (id,
                                ios_app_id,
                                title,
                                developer_name,
                                developer_ios_id,
                                ios_store_url,
                                seller_official_website,
                                age_rating,
                                total_average_rating,
                                total_number_of_ratings,
                                average_rating_for_version,
                                number_of_ratings_for_version,
                                original_release_date,
                                current_version_release_date,
                                price_usd,
                                primary_genre,
                                all_genres,
                                languages,
                                description)
                        VALUES (RS.id,
                                RS.ios_app_id,
                                RS.title,
                                RS.developer_name,
                                RS.developer_ios_id,
                                RS.ios_store_url,
                                RS.seller_official_website,
                                RS.age_rating,
                                RS.total_average_rating,
                                RS.total_number_of_ratings,
                                RS.average_rating_for_version,
                                RS.number_of_ratings_for_version,
                                RS.original_release_date,
                                RS.current_version_release_date,
                                RS.price_usd,
                                RS.primary_genre,
                                RS.all_genres,
                                RS.languages,
                                RS.description)""",
    )

    stream_to_master = IosAppsInfoSnowflakeOperator(
        task_id="stream_to_master",
        sql="""MERGE INTO TEST_DB.TEST_SH.MASTER_TABLE MT
                USING TEST_DB.TEST_SH.STAGE_STREAM SS
                ON MT.id = SS.id
                WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE'
                    THEN DELETE
                WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'
                    THEN UPDATE SET MT.id = SS.id,
                                    MT.ios_app_id = SS.ios_app_id,
                                    MT.title = SS.title,
                                    MT.developer_name = SS.developer_name,
                                    MT.developer_ios_id = SS.developer_ios_id,
                                    MT.ios_store_url = SS.ios_store_url,
                                    MT.seller_official_website = SS.seller_official_website,
                                    MT.age_rating = SS.age_rating,
                                    MT.total_average_rating = SS.total_average_rating,
                                    MT.total_number_of_ratings = SS.total_number_of_ratings,
                                    MT.average_rating_for_version = SS.average_rating_for_version,
                                    MT.number_of_ratings_for_version = SS.number_of_ratings_for_version,
                                    MT.original_release_date = SS.original_release_date,
                                    MT.current_version_release_date = SS.current_version_release_date,
                                    MT.price_usd = SS.price_usd,
                                    MT.primary_genre = SS.primary_genre,
                                    MT.all_genres = SS.all_genres,
                                    MT.languages = SS.languages,
                                    MT.description = SS.description
                WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
                    THEN INSERT (id,
                                ios_app_id,
                                title,
                                developer_name,
                                developer_ios_id,
                                ios_store_url,
                                seller_official_website,
                                age_rating,
                                total_average_rating,
                                total_number_of_ratings,
                                average_rating_for_version,
                                number_of_ratings_for_version,
                                original_release_date,
                                current_version_release_date,
                                price_usd,
                                primary_genre,
                                all_genres,
                                languages,
                                description)
                        VALUES (SS.id,
                                SS.ios_app_id,
                                SS.title,
                                SS.developer_name,
                                SS.developer_ios_id,
                                SS.ios_store_url,
                                SS.seller_official_website,
                                SS.age_rating,
                                SS.total_average_rating,
                                SS.total_number_of_ratings,
                                SS.average_rating_for_version,
                                SS.number_of_ratings_for_version,
                                SS.original_release_date,
                                SS.current_version_release_date,
                                SS.price_usd,
                                SS.primary_genre,
                                SS.all_genres,
                                SS.languages,
                                SS.description)""",
    )

    stage_file >> load_to_raw >> stream_to_stage >> stream_to_master  # pylint: disable=W0104


ios_apps_info()
