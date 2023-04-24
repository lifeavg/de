import json
from typing import Dict, List, Union

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.utils.email import send_email
from globals import ALERT_EMAIL, COLUMNS
from tasks import get_file_names

ALERT_BORDER = 10


def _to_alert_list(data: pd.Series) -> List[Dict[str, Union[str, int]]]:
    """
    convert Series with group index to JSON compatible list of dicts
    """
    alerts: List[Dict[str, Union[str, int]]] = []
    for index in data.index:
        alerts.append(
            {
                "bundle_id": str(index[0]),
                "start": pd.Timestamp(
                    year=index[1],
                    month=index[2],
                    day=index[3],
                    hour=index[4],
                ).isoformat(),
                "end": pd.Timestamp(
                    year=index[1],
                    month=index[2],
                    day=index[4],
                    hour=index[4] + 1,
                ).isoformat(),
                "errorCount": int(data[index]),  # type: ignore
            }
        )
    return alerts


@task()
def count_fatal_errors(files_data: str) -> str:
    # load file paths from xCom data
    files: List[str] = json.loads(files_data)
    if not files:
        return "[]"
    # create data from from csv
    data = pd.concat(
        (pd.read_csv(file, names=COLUMNS) for file in files), ignore_index=True
    )
    # filter errors
    data = data[data["severity"] == "Error"]
    # convert error timestamp to date
    data["date"] = pd.to_datetime(data["date"], unit="s")
    # group errors and count
    data = data.groupby(
        [
            data["bundle_id"],
            data["date"].dt.year,
            data["date"].dt.month,
            data["date"].dt.day,
            data["date"].dt.hour,
        ]
    ).size()
    # filter minutes exiting alert border
    data = data.where(data > ALERT_BORDER).dropna()
    return json.dumps(_to_alert_list(data))


@task(provide_context=True)
def send_emails(alert_data: str, **kwargs) -> None:
    """
    send email for each alert
    """
    alerts: List[Dict[str, Union[str, int]]] = json.loads(alert_data)
    for alert in alerts:
        send_email(
            to=ALERT_EMAIL,
            subject=f"Alert {kwargs['dag'].dag_id}!",
            html_content=f"Alert {kwargs['dag'].dag_id}!\nIn period from {alert['start']} "
            f"to {alert['end']}\n{alert['errorCount']} errors received for {alert['bundle_id']}.",
        )


@dag(
    start_date=pendulum.datetime(2023, 4, 24, 13, 40, tz="UTC"),
    schedule_interval="0 * * * *",
)
def bundle_id_fatal_error_tracker():
    file_names = get_file_names()
    fatal_errors = count_fatal_errors(file_names)
    emails = send_emails(fatal_errors)
    file_names >> fatal_errors >> emails  # pylint: disable=W0104


bundle_id_fatal_error_tracker()
