from prefect_aws import S3Bucket
import pandas as pd
import tempfile
from pathlib import Path

from typing import Optional


def get_file_uri(
    data_path: Path | str, bucket_block_name: str = "million-songs-dataset-s3"
) -> str:
    bucket = S3Bucket.load(bucket_block_name)
    return f"s3://{bucket.bucket_name}/{data_path}"


def upload_dir_to_s3(
    data_dir: Path | str,
    target_dir: Optional[Path | str],
    bucket_block_name: str = "million-songs-dataset-s3",
) -> int:
    bucket = S3Bucket.load(bucket_block_name)
    to_path = str(target_dir) if target_dir is not None else None
    file_count = bucket.put_directory(local_path=str(data_dir), to_path=to_path)
    return file_count


def upload_file_to_s3(
    data_path: Path | str,
    to_path: Path | str,
    bucket_block_name: str = "million-songs-dataset-s3",
) -> None:
    bucket = S3Bucket.load(bucket_block_name)
    bucket.upload_from_path(str(data_path), str(to_path))


def download_file_from_s3(
    data_path: Path | str,
    to_path: Path | str,
    bucket_block_name: str = "million-songs-dataset-s3",
) -> None:
    bucket = S3Bucket.load(bucket_block_name)
    bucket.download_object_to_path(data_path, to_path)


def read_parquet_data(
    data_path: Path | str, bucket_block_name: str = "million-songs-dataset-s3"
) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(mode="w") as f:
        download_file_from_s3(data_path, f.name, bucket_block_name)
        data = pd.read_parquet(f.name)
    return data


def write_parquet_data(
    df: pd.DataFrame,
    to_path: Path | str,
    bucket_block_name: str = "million-songs-dataset-s3",
) -> None:
    with tempfile.NamedTemporaryFile(mode="w") as f:
        df.to_parquet(f.name)
        upload_file_to_s3(f.name, to_path, bucket_block_name)
