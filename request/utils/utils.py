import json

import boto3


def read_json(bucket_name: str, path: str) -> dict:
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_name=bucket_name, key=path)
    data = obj.get()["Body"].read().decode("utf-8")
    return json.loads(data)
