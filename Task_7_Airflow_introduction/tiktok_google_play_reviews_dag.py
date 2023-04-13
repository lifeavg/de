"""
enable_xcom_pickling = True
for real DAG better not to use due to security issues. Use JSON
Or saving every task (stage) result to external DB and load from DB on next task
Or even better not to use orchestrator for data processing

For MongoDB connection used:
backend = airflow.secrets.local_filesystem.LocalFilesystemBackend
backend_kwargs = {"variables_file_path": "/home/lifeavg/airflow/secrets/var.json", "connections_file_path": "/home/lifeavg/airflow/secrets/conn.json"}

var.json content: {}
conn.json content: {"test_mongo": "mongodb://mongoadmin:mongosecret@localhost:27017/raitings?authSource=admin"} // admin profile for test only

transform group divided into separate tasks only to practice with TaskGroup functionality
"""
from pathlib import Path
from string import punctuation as _punctuation
from unicodedata import normalize

import numpy as np
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

FILE_DIR = Path("/home/lifeavg/Documents/projects/de/Task_7_Airflow_introduction/data")
FILE = FILE_DIR / Path("tiktok_google_play_reviews.csv")


punctuation = np.array(list(_punctuation + " "))


def filter_content_string(string: str) -> str:
    """
    normalize symbols to compact form.
    remove everything except letters, numbers and punctuation marks
    """
    # normalize string to compact form
    chars = np.array(list(normalize("NFC", string)))
    # mask only letters and numbers
    is_letter_num = np.char.isalnum(chars)
    # mask only punctuation marks and spaces
    is_punctuation = np.isin(chars, punctuation)
    # combine masks to chose values from string
    mask = np.logical_or(is_letter_num, is_punctuation)
    # apply mask and remove duplicated spaces
    return " ".join("".join(chars[mask]).split())


@task
def extract() -> pd.DataFrame:
    return pd.read_csv(FILE, parse_dates=["at", "repliedAt"])


@task
def filter_content(data: pd.DataFrame) -> pd.DataFrame:
    data["content"] = data["content"].astype("str").map(filter_content_string)
    return data


@task
def sort_by_creation_date(data: pd.DataFrame) -> pd.DataFrame:
    return data.sort_values("at")


@task
def fill_nulls(data: pd.DataFrame) -> pd.DataFrame:
    return data.fillna("-")


@task
def load(data: pd.DataFrame):
    with MongoHook(conn_id="test_mongo") as mongo:
        docs = data.to_dict("records")
        filters = [{"reviewId": d["reviewId"]} for d in docs]
        mongo.replace_many(mongo_collection="reviews", docs=docs, upsert=True, filter_docs=filters)


@dag(
    start_date=pendulum.datetime(2023, 4, 13, 7, 35, tz="UTC"),
    schedule_interval="*/3 * * * *",
    tags=["tiktok", "google_play", "reviews"],
    description="when tiktok_google_play_reviews.csv appears in target directory try to load data to mongoDB",
)
def tiktok_google_play_reviews():
    data_file_sensor = FileSensor(
        task_id="wait_tiktok_google_play_reviews_csv",
        filepath=FILE_DIR,
        poke_interval=300,
        mode="reschedule",
        timeout=3000,
    )
    extracted = extract()
    with TaskGroup("transform") as transform:
        filtered = filter_content(extracted)
        sorted_ = sort_by_creation_date(filtered)
        filled = fill_nulls(sorted_)
        filtered >> sorted_ >> filled  # pylint: disable=W0104

    data_file_sensor >> extracted >> transform >> load(filled)  # pylint: disable=W0106


tiktok_google_play_reviews()
