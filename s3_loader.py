import pandas as pd
import glob as glob
import boto3
import io
from botocore import UNSIGNED
from botocore.config import Config


def make_s3(profile=None):
    if profile:
        session = boto3.Session(profile_name=profile)
        s3 = session.client("s3")
    else:
        # Default anonymous client
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    return s3


def read_s3_file(file_path, sep, in_staging=False, profile=None):
    s3 = make_s3(profile=profile)
    if in_staging:
        bucket = 'staging-cellpainting-gallery'
    else:
        bucket = 'cellpainting-gallery'
    response = s3.get_object(Bucket=bucket, Key=file_path)
    raw = response["Body"].read()
    if file_path.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(raw)).astype(str)
    elif file_path.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(raw)).astype(str)
    else:
        try:
            df = pd.read_csv(io.BytesIO(raw), sep=sep, encoding="utf-8").astype(str)
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(io.BytesIO(raw), sep=sep, encoding="latin1").astype(
                    str
                )
            except pd.errors.ParserError:
                try:
                    df = pd.read_csv(
                        io.BytesIO(raw), sep=None, engine="python", encoding="latin1"
                    ).astype(str)
                except:
                    print(f"Failed to read {file_path}.")
    return df


def parse_s3_folder(prefix, in_staging=False, profile=None):
    s3 = make_s3(profile=profile)
    parsed_files = []
    paginator = s3.get_paginator("list_objects_v2")
    if in_staging:
        bucket = 'staging-cellpainting-gallery'
    else:
        bucket = 'cellpainting-gallery'
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        for obj in page.get("Contents", []):
            parsed_files.append(obj["Key"])
    return parsed_files
