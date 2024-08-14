import asyncio
import datetime
import os
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import pytest
from mlflow.client import MlflowClient
from prefect.testing.utilities import prefect_test_harness

from genre_classifier.blocks.create_aws_credentials import create_aws_creds_block
from genre_classifier.blocks.create_s3_buckets import create_s3_buckets
from genre_classifier.flows.ingest_data.flow import upload_to_s3
from genre_classifier.flows.predict.flow import predict_flow
from genre_classifier.flows.preprocess.flow import preprocess_flow
from genre_classifier.flows.split_data.flow import split_data_flow
from genre_classifier.flows.train.flow import train_flow
from genre_classifier.utils import read_parquet_data

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
    # response = _client.list_objects(
    #     Bucket="million-songs-dataset-cicd", Prefix=str(s3_base_directory)
    # )
    # if "Contents" not in response:
    #     return
    # for track in response["Contents"]:
    #     _client.delete_object(Bucket="million-songs-dataset-cicd", Key=track["Key"])


@pytest.fixture(scope="session")
def cicd_data(s3_client) -> Path:
    with tempfile.TemporaryDirectory() as tmpdir:
        archive_local_path = Path(tmpdir) / "msd-integration.tar.gz"
        s3_client.download_file(
            "msd-integration-test-data", "msd-integration.tar.gz", archive_local_path
        )
        os.system(f"tar -xzf {archive_local_path} -C {tmpdir}")
        yield Path(tmpdir) / "MillionSongSubset"


@pytest.fixture(scope="session")
def mlflow_client() -> MlflowClient:
    return MlflowClient("http://0.0.0.0:5000")


@pytest.fixture(scope="session")
def experiment_id(mlflow_client) -> str:
    try:
        experiment_id = mlflow_client.get_experiment_by_name(
            "integration-test"
        ).experiment_id
        return experiment_id
    except Exception:
        return mlflow_client.create_experiment("integration-test")


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

    # Read the data again
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


def test_train(mlflow_client, experiment_id):
    train_flow(
        mlflow_experiment_name="integration-test",
        mlflow_tracking_uri="http://0.0.0.0:5000",
        bucket_block_name="million-songs-dataset-s3-cicd",
        data_path=str(s3_base_directory / "subset" / "splits"),
        register_model_if_accepted=True,
        max_hamming_loss=1.0,
        min_jaccard_score=0.0,
        register_to_environment="integration-test",
    )

    run = mlflow_client.search_runs(experiment_ids=[experiment_id])[0]

    assert run.info.status == "FINISHED"

    model = mlflow_client.get_registered_model("genre-classifier-random-forest")
    assert len(model.latest_versions) == 1
    assert model.latest_versions[0].tags.get("env") == "integration-test"

    model = mlflow_client.get_registered_model("genre-classifier-multi-label-binarizer")
    assert len(model.latest_versions) == 1
    assert model.latest_versions[0].tags.get("env") == "integration-test"


def test_predict(mlflow_client, s3_client):
    predictions_data_path = str(s3_base_directory / "subset" / "predictions")

    predict_flow(
        bucket_block_name="million-songs-dataset-s3-cicd",
        source_data_path=str(s3_base_directory / "subset" / "splits" / "daily"),
        target_data_path=predictions_data_path,
        environment="integration-test",
    )

    # Check if the predictions are created
    response = s3_client.list_objects(
        Bucket="million-songs-dataset-cicd", Prefix=predictions_data_path
    )
    assert "Contents" in response
    assert len(response["Contents"]) == 1
    assert response["Contents"][0]["Key"].endswith("predictions.parquet")

    # Check if the predictions are a valid dataframe with genre predictions
    predictions_df: pd.DataFrame = read_parquet_data(
        response["Contents"][0]["Key"],
        bucket_block_name="million-songs-dataset-s3-cicd",
    )
    print(predictions_df.head())
    assert len(predictions_df) == 10  # 10 releases per day
    assert predictions_df.columns.tolist() == ["genres"]
