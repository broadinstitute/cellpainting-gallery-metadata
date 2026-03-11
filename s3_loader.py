import pandas as pd
from matplotlib import pyplot as plt
import glob as glob
import boto3
import io
from botocore import UNSIGNED
from botocore.config import Config


def make_s3():
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    return s3


def read_s3_file(file_path, sep):
    s3 = make_s3()
    response = s3.get_object(Bucket="cellpainting-gallery", Key=file_path)
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


def parse_s3_folder(prefix):
    s3 = make_s3()
    parsed_files = []
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket="cellpainting-gallery", Prefix=prefix)
    for page in pages:
        for obj in page.get("Contents", []):
            parsed_files.append(obj["Key"])
    return parsed_files


if __name__ == "__main__":

    read_s3_file(
        "cpg0002-jump-scope/source_4/workspace/load_data_csv/2020_10_27_Scope1_YokogawaJapan/20201020T134356/load_data.csv"
    )
