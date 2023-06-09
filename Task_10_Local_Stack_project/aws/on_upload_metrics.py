import os
import re

import boto3

endpoint_url = f'http://{os.getenv("LOCALSTACK_HOSTNAME")}:{os.getenv("EDGE_PORT")}'
client = boto3.client("s3", endpoint_url=endpoint_url)
regex = re.compile(r"helsinki_bikes_[0-9]{4}_[0-9]{1,2}_metrics/")


def on_upload_metrics(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    if re.match(regex, key):
        folder_key, file, *_ = key.split("/")
        if file == "_temporary":
            return
        if file.endswith(".csv"):
            client.copy_object(Bucket=bucket, CopySource="/".join((bucket, key)), Key=folder_key + ".csv")
            client.delete_object(
                Bucket=bucket,
                Key=key,
            )
        if file == "_SUCCESS":
            client.delete_object(
                Bucket=bucket,
                Key=key,
            )
