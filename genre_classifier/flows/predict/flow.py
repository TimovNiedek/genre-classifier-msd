import mlflow
import pandas as pd
from mlflow.client import MlflowClient
from prefect import flow, get_run_logger, task
from prefect_aws import S3Bucket
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MultiLabelBinarizer

from genre_classifier.preprocess_common import fix_outliers
from genre_classifier.utils import (
    read_parquet_data,
    set_aws_credential_env,
    write_parquet_data,
)


@task
def fetch_model(registered_model_name: str, env="production"):
    set_aws_credential_env("aws-creds")
    client = MlflowClient("http://127.0.0.1:5000")

    model = client.get_registered_model(registered_model_name)
    model_version = [
        model for model in model.latest_versions if model.tags.get("env") == env
    ][0]
    return mlflow.sklearn.load_model(model_version.source)


@task
def get_latest_releases(
    bucket_block_name, source_data_path, target_data_path
) -> tuple[pd.DataFrame, str] | None:
    """Get the latest releases for which predictions were not yet made"""
    logger = get_run_logger()

    bucket = S3Bucket.load(bucket_block_name)
    release_directories = list(
        sorted(
            [obj["Key"].split("/")[-2] for obj in bucket.list_objects(source_data_path)]
        )
    )
    prediction_directories = list(
        sorted(
            [obj["Key"].split("/")[-2] for obj in bucket.list_objects(target_data_path)]
        )
    )

    if len(prediction_directories) == len(release_directories):
        return None

    latest_date = release_directories[len(prediction_directories)]
    data_path = f"{source_data_path}/{latest_date}/releases.parquet"
    logger.info(f"Fetching data for date {latest_date} from path {data_path}")
    df = read_parquet_data(data_path, bucket_block_name=bucket_block_name)
    logger.info(f"Found {len(df)} rows")
    return df, latest_date


@task
def predict(
    df: pd.DataFrame,
    pipeline: Pipeline,
    mlb: MultiLabelBinarizer,
    valid_tempo_min: float = 70,
    valid_tempo_max: float = 180,
) -> pd.DataFrame:
    df = fix_outliers(df, valid_tempo_min, valid_tempo_max)
    predictions = pipeline.predict(df)
    predictions_plaintext = mlb.inverse_transform(predictions)
    prediction_data = []
    for song_id, pred_genres in zip(df.index, predictions_plaintext):
        prediction_data.append({"song_id": song_id, "genres": list(pred_genres)})
    prediction_df = pd.DataFrame(prediction_data).set_index("song_id")
    return prediction_df


@task
def upload_predictions(
    df: pd.DataFrame,
    target_data_path: str,
    bucket_block_name: str = "million-songs-dataset-s3",
):
    write_parquet_data(df, target_data_path, bucket_block_name)


@flow(log_prints=True)
def predict_flow(
    bucket_block_name: str = "million-songs-dataset-s3",
    source_data_path: str = "subset/daily",
    target_data_path: str = "subset/predictions",
    valid_tempo_min: float = 70,
    valid_tempo_max: float = 180,
    environment: str = "dev",
):
    logger = get_run_logger()
    releases_data = get_latest_releases(
        bucket_block_name, source_data_path, target_data_path
    )
    if releases_data is None:
        logger.info("Predictions are up to date, nothing to do.")
        return
    else:
        releases, date = releases_data
    pipeline = fetch_model("genre-classifier-random-forest", environment)
    mlb = fetch_model("genre-classifier-multi-label-binarizer", environment)
    predictions_data = predict(
        releases, pipeline, mlb, valid_tempo_min, valid_tempo_max
    )
    predictions_fullpath = f"{target_data_path}/{date}/predictions.parquet"
    upload_predictions(predictions_data, predictions_fullpath, bucket_block_name)


if __name__ == "__main__":
    predict_flow()
