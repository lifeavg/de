import io
import json
import os
import re
from string import ascii_letters
from uuid import uuid4

import boto3
import pandas as pd
from botocore.exceptions import ClientError

endpoint_url = f'http://{os.getenv("LOCALSTACK_HOSTNAME")}:{os.getenv("EDGE_PORT")}'
regex_metrics = re.compile(r"^helsinki_bikes_[0-9]{4}_[0-9]{1,2}_metrics\.csv$")
regex_raw_data = re.compile(r"^helsinki_bikes_[0-9]{4}_[0-9]{1,2}\.csv$")

s3 = boto3.client("s3", endpoint_url=endpoint_url)
dynamodb = boto3.client("dynamodb", endpoint_url=endpoint_url)


def load_event(event_data):
    return json.loads(event_data["body"])


def get_key_bucket(event):
    return (event["Records"][0]["s3"]["object"]["key"], event["Records"][0]["s3"]["bucket"]["name"])


def get_year_mont_from_key(key):
    _, _, year, month, *other = key.split(".")[0].split("_")
    return year, month


def dynamodb_input_item_from_dict(data):
    item = {}
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, int) or isinstance(value, float):
            item[key] = {"N": str(value)}
        else:
            item[key] = {"S": value}
    return item


def normalize_keys(keys):
    normalized_keys = []
    for key in keys:
        ascii_filtered_chars = []
        for char in key:
            if char not in ascii_letters:
                ascii_filtered_chars.append(" ")
            else:
                ascii_filtered_chars.append(char)
        ascii_filtered = "".join(ascii_filtered_chars).split()
        capitalized = []
        for filtered_key in ascii_filtered:
            capitalized.append(filtered_key[0].capitalize() + filtered_key[1:])
        normalized_keys.append("".join(capitalized))
    return normalized_keys


def generate_dynamodb_item_butches(dataframe_rows, bath_size):
    butch = []
    for index, row in dataframe_rows:
        butch.append(
            {
                "PutRequest": {
                    "Item": dynamodb_input_item_from_dict(row.to_dict()),
                },
            }
        )
        if len(butch) == bath_size:
            yield butch
            butch = []
    if butch:
        yield butch


def normalize_metrics_dataframe(dataframe, year, month):
    dataframe.columns = normalize_keys(dataframe.columns)
    dataframe["Month"] = f"{year}-{month}-01"
    return dataframe.fillna(0)


def normalize_raw_data_dataframe(dataframe):
    dataframe.columns = normalize_keys(dataframe.columns)
    dataframe["Index"] = dataframe.apply(lambda _: str(uuid4()), axis=1)
    return dataframe.fillna(0)


def write_data_to_dynamodb(dataframe, table, batch_size):
    for batch in generate_dynamodb_item_butches(dataframe.iterrows(), batch_size):
        try:
            dynamodb.batch_write_item(
                RequestItems={
                    table: batch,
                },
            )
        except Exception as e:
            print(batch)
            print(e)


def calculate_daily_averages(raw_data_dataframe):
    raw_data_dataframe["Day"] = raw_data_dataframe["Departure"].str.split(expand=True)[0]
    return raw_data_dataframe[["Day", "DistanceM", "DurationSec", "AvgSpeedKmH", "AirTemperatureDegC"]].groupby(
        "Day").mean().reset_index()


def process_data_objects(metrics_object, raw_data_object, year, month):
    metrics_dataframe = pd.read_csv(io.BytesIO(metrics_object["Body"].read()))
    metrics_dataframe = normalize_metrics_dataframe(metrics_dataframe, year=year, month=month)
    write_data_to_dynamodb(metrics_dataframe, "HelsinkiCityBikesStationMonthMetrics", 25)
    raw_data_dataframe = pd.read_csv(io.BytesIO(raw_data_object["Body"].read()))
    raw_data_dataframe = normalize_raw_data_dataframe(raw_data_dataframe)
    daily_averages_dataframe = calculate_daily_averages(raw_data_dataframe)
    write_data_to_dynamodb(daily_averages_dataframe, "HelsinkiCityBikesDayMetrics", 25)
    write_data_to_dynamodb(raw_data_dataframe, "HelsinkiCityBikes", 10)


def s3_to_dynamodb(events, context):
    for event_data in events["Records"]:
        event = load_event(event_data)
        key, bucket = get_key_bucket(event)
        if re.match(regex_raw_data, key):
            year, month = get_year_mont_from_key(key)
            raw_data_object = s3.get_object(Bucket=bucket, Key=key)
            try:
                metrics_object = s3.get_object(Bucket=bucket, Key=f"helsinki_bikes_{year}_{month}_metrics.csv")
            except ClientError as exception:
                if exception.response["Error"]["Code"] == "NoSuchKey":
                    return
                else:
                    raise exception
            else:
                process_data_objects(metrics_object, raw_data_object, year, month)
        elif re.match(regex_metrics, key):
            year, month = get_year_mont_from_key(key)
            metrics_object = s3.get_object(Bucket=bucket, Key=key)
            try:
                raw_data_object = s3.get_object(Bucket=bucket, Key=f"helsinki_bikes_{year}_{month}.csv")
            except ClientError as exception:
                if exception.response["Error"]["Code"] == "NoSuchKey":
                    return
                else:
                    raise exception
            else:
                process_data_objects(metrics_object, raw_data_object, year, month)
