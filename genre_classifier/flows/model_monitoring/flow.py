import datetime
import tempfile

import numpy as np
import pandas as pd
from evidently import ColumnMapping
from evidently.metric_preset import DataDriftPreset
from evidently.report import Report
from prefect import flow, get_run_logger, task
from prefect_aws import S3Bucket

from genre_classifier.flows.complete_training.flow import complete_training_flow
from genre_classifier.utils import get_file_uri, read_parquet_data, upload_file_to_s3

FEATURE_COLS = ["duration", "key", "loudness", "mode", "tempo", "year"]
NUMERICAL_COLS = ["duration", "loudness", "tempo", "year"]
BINARY_COLS = ["mode"]
CATEGORICAL_COLS = ["key"]
LABEL_COL = "genres"


@task
def get_reference_data(bucket_block_name="million-songs-dataset-s3") -> pd.DataFrame:
    return read_parquet_data(
        "subset/train.parquet", bucket_block_name=bucket_block_name
    )


@task
def get_ground_truth_data(bucket_block_name="million-songs-dataset-s3") -> pd.DataFrame:
    return read_parquet_data("subset/test.parquet", bucket_block_name=bucket_block_name)


@task
def calculate_metrics(
    reference: pd.DataFrame,
    ground_truth: pd.DataFrame,
    bucket_block_name="million-songs-dataset-s3",
) -> Report:
    bucket = S3Bucket.load(bucket_block_name)
    all_features = []

    input_files = bucket.list_objects("subset/daily")
    for input_file_object in input_files:
        input_file_path = input_file_object["Key"]
        pred_date = input_file_path.split("/")[-2]
        features_df = read_parquet_data(input_file_path)
        features_df["timestamp"] = datetime.datetime.strptime(pred_date, "%Y-%m-%d")
        all_features.append(features_df)

    all_features_df = pd.concat(all_features)
    reference["year"].replace(0, np.nan)
    reference["timestamp"] = datetime.datetime(year=2024, month=1, day=1)

    column_mapping = ColumnMapping()
    column_mapping.numerical_features = NUMERICAL_COLS
    column_mapping.categorical_features = BINARY_COLS + CATEGORICAL_COLS
    column_mapping.datetime = "timestamp"
    column_mapping.id = "song_id"
    column_mapping.target = None

    data_drift_report = Report(
        metrics=[
            DataDriftPreset(),
        ]
    )

    data_drift_report.run(
        reference_data=reference,
        current_data=all_features_df,
        column_mapping=column_mapping,
    )
    print(data_drift_report)

    with tempfile.TemporaryDirectory() as dir:
        report_path = f"{dir}/index.html"
        json_path = f"{dir}/report.json"
        data_drift_report.save_json(json_path)
        data_drift_report.save_html(report_path)
        upload_file_to_s3(
            report_path,
            to_path="index.html",
            bucket_block_name="evidently-static-dashboard",
            ExtraArgs={"ContentType": "text/html"},
        )
        upload_file_to_s3(
            json_path,
            to_path="report.json",
            bucket_block_name="evidently-static-dashboard",
            ExtraArgs={"ContentType": "application/json"},
        )

    return data_drift_report


@task
def validate_model_performance(report: Report) -> bool:
    report_dict = report.as_dict()
    return report_dict["metrics"][0]["result"]["dataset_drift"]


@flow
def model_monitoring_flow(
    bucket_block_name: str = "million-songs-dataset-s3",
    trigger_retrain_if_needed: bool = True,
) -> bool:
    logger = get_run_logger()
    reference = get_reference_data(bucket_block_name=bucket_block_name)
    ground_truth = get_ground_truth_data(bucket_block_name=bucket_block_name)
    report = calculate_metrics(
        reference, ground_truth, bucket_block_name=bucket_block_name
    )
    retrain_needed = validate_model_performance(report)
    if retrain_needed:
        logger.info("Model should be retrained!")
        report_url = get_file_uri("subset/metrics_report.html", bucket_block_name)
        logger.info(f"Full report available at {report_url}")
        if trigger_retrain_if_needed:
            logger.info("Triggering complete training run")
            complete_training_flow(mlflow_experiment_name="automatic-retraining")
    else:
        logger.info("No retrain required")
    return retrain_needed


if __name__ == "__main__":
    model_monitoring_flow()
