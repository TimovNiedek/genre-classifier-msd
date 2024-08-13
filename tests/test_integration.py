import os

import pandas as pd
import pytest
import boto3
import tempfile
from pathlib import Path

from genre_classifier.flows.ingest_data.flow import upload_to_s3
from prefect.testing.utilities import prefect_test_harness
from genre_classifier.blocks.create_aws_credentials import create_aws_creds_block
from genre_classifier.blocks.create_s3_buckets import create_s3_buckets
from genre_classifier.utils import read_parquet_data

from genre_classifier.flows.preprocess.flow import preprocess_flow
from genre_classifier.flows.split_data.flow import split_data_flow
import asyncio

import datetime

current_timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d-%H-%M-%S")
s3_base_directory = Path(current_timestamp)


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        create_aws_creds_block()
        create_s3_buckets()
        yield


@pytest.fixture(autouse=True, scope="session")
def s3_client():
    _client = boto3.client("s3")
    yield _client

    # cleanup the bucket
    response = _client.list_objects(
        Bucket="million-songs-dataset-cicd", Prefix=str(s3_base_directory)
    )
    for track in response["Contents"]:
        _client.delete_object(Bucket="million-songs-dataset-cicd", Key=track["Key"])


@pytest.fixture(scope="session")
def cicd_data(s3_client) -> Path:
    with tempfile.TemporaryDirectory() as tmpdir:
        archive_local_path = Path(tmpdir) / "msd-integration.tar.gz"
        s3_client.download_file(
            "msd-integration-test-data", "msd-integration.tar.gz", archive_local_path
        )
        os.system(f"tar -xzf {archive_local_path} -C {tmpdir}")
        yield Path(tmpdir) / "MillionSongSubset"


def test_upload_to_s3(cicd_data, s3_client):
    assert Path(cicd_data).exists()
    track_files = list(cicd_data.rglob("*.h5"))
    print(f"Number of files: {len(track_files)}")
    result = upload_to_s3(
        cicd_data,
        s3_base_directory / "subset",
        bucket_block_name="million-songs-dataset-s3-cicd",
    )
    assert result == len(track_files)

    # Check if the files are uploaded
    response = s3_client.list_objects(
        Bucket="million-songs-dataset-cicd", Prefix=str(s3_base_directory / "subset")
    )
    assert "Contents" in response
    assert len(response["Contents"]) == len(track_files)


def test_preprocess_flow(s3_client):
    asyncio.run(
        preprocess_flow(
            bucket_folder=str(s3_base_directory / "subset"),
            target_path=str(s3_base_directory / "subset" / "subset.parquet"),
            s3_bucket_block_name="million-songs-dataset-s3-cicd",
            limit=200,
        )
    )

    # Check if the parquet file exists
    preprocessed_data_path = str(s3_base_directory / "subset" / "subset.parquet")
    response = s3_client.list_objects(
        Bucket="million-songs-dataset-cicd", Prefix=preprocessed_data_path
    )
    assert "Contents" in response
    assert len(response["Contents"]) == 1

    # Read the data again and make sure it is a valid dataframe
    df: pd.DataFrame = read_parquet_data(
        preprocessed_data_path, bucket_block_name="million-songs-dataset-s3-cicd"
    )
    assert len(df) == 200
    assert df.columns.tolist() == [
        "song_id",
        "danceability",
        "duration",
        "energy",
        "key",
        "loudness",
        "mode",
        "tempo",
        "year",
        "genres",
    ]


def test_split_data_flow(s3_client):
    split_data_flow(
        bucket_block_name="million-songs-dataset-s3-cicd",
        source_data_path=str(s3_base_directory / "subset" / "subset.parquet"),
        target_data_path=str(s3_base_directory / "subset" / "splits"),
        val_size=0.1,
        test_size=0.1,
        seed=42,
        new_releases_start_date=datetime.date(2022, 1, 1),
        num_releases_per_day=10,
    )

    # Check if the train, val and test sets are created
    train_data_path = str(s3_base_directory / "subset" / "splits" / "train.parquet")
    val_data_path = str(s3_base_directory / "subset" / "splits" / "val.parquet")
    test_data_path = str(s3_base_directory / "subset" / "splits" / "test.parquet")

    for data_path in [train_data_path, val_data_path, test_data_path]:
        response = s3_client.list_objects(
            Bucket="million-songs-dataset-cicd", Prefix=data_path
        )
        assert "Contents" in response
        assert len(response["Contents"]) == 1

    # Read the data againx
    train_df: pd.DataFrame = read_parquet_data(
        train_data_path, bucket_block_name="million-songs-dataset-s3-cicd"
    )
    val_df: pd.DataFrame = read_parquet_data(
        val_data_path, bucket_block_name="million-songs-dataset-s3-cicd"
    )
    test_df: pd.DataFrame = read_parquet_data(
        test_data_path, bucket_block_name="million-songs-dataset-s3-cicd"
    )

    assert len(train_df) == 200 * 0.8
    assert len(val_df) == 200 * 0.1
    assert len(test_df) == 200 * 0.1

    # Check if the daily releases are created
    daily_releases_path = str(s3_base_directory / "subset" / "splits" / "daily")
    response = s3_client.list_objects(
        Bucket="million-songs-dataset-cicd", Prefix=daily_releases_path
    )
    assert "Contents" in response
    assert len(response["Contents"]) == 2
