from prefect import flow, task, get_run_logger
from genre_classifier.utils import (
    read_parquet_data,
    get_file_uri,
    set_aws_credential_env,
)
from genre_classifier.preprocess_common import fix_outliers as _fix_outliers
from sklearn.impute import KNNImputer
from sklearn.preprocessing import MinMaxScaler, MultiLabelBinarizer
from sklearn.compose import make_column_transformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import make_pipeline, Pipeline
from sklearn.metrics import jaccard_score, hamming_loss
from mlflow.models import infer_signature

import numpy as np
import pandas as pd

import mlflow
import mlflow.data
from tempfile import TemporaryDirectory

from pathlib import Path


FEATURE_COLS = ["duration", "key", "loudness", "mode", "tempo", "year"]
NUMERICAL_COLS = ["duration", "loudness", "tempo", "year"]
BINARY_COLS = ["mode"]
CATEGORICAL_COLS = ["key"]
LABEL_COL = "genres"


@task
def read_data(
    data_path: str, bucket_block_name: str = "million-songs-dataset-s3"
) -> pd.DataFrame:
    data_uri = get_file_uri(data_path, bucket_block_name=bucket_block_name)
    data = read_parquet_data(data_path, bucket_block_name)
    dataset = mlflow.data.from_pandas(data, source=data_uri, name=Path(data_path).stem)
    mlflow.log_input(dataset, context="training")
    return data


@task
def get_top_genres(df: pd.DataFrame, k=50) -> list[str]:
    genre_counts = df["genres"].explode().value_counts()
    genre_names = list(genre_counts.index)
    top_genre_names = genre_names[:k]
    with TemporaryDirectory() as tmpdir:
        genres_file = Path(tmpdir) / "genres.txt"
        with open(genres_file, "w") as f:
            f.write("\n".join(top_genre_names))

        mlflow.log_artifact(genres_file)
    return top_genre_names


@task
def filter_top_genres(df: pd.DataFrame, genre_names: list[str]) -> pd.DataFrame:
    df["genres_filtered"] = df["genres"].apply(
        lambda genres: [genre for genre in genres if genre in genre_names]
    )
    data_filtered = df[df["genres_filtered"].map(len) > 0]
    data_filtered = data_filtered.drop("genres", axis=1).rename(
        columns={"genres_filtered": "genres"}
    )

    return data_filtered


def fix_tempo(tempo_val: float, min_tempo: float, max_tempo: float) -> float:
    if tempo_val == 0:
        return np.nan
    elif tempo_val > max_tempo:
        return tempo_val / 2
    elif tempo_val < min_tempo:
        return tempo_val * 2
    return tempo_val


@task
def fix_outliers(
    df: pd.DataFrame, valid_tempo_min: float = 70, valid_tempo_max: float = 180
) -> pd.DataFrame:
    logger = get_run_logger()
    logger.debug("Before fixing outliers:")
    logger.debug(df.describe())
    df = _fix_outliers(df, valid_tempo_min, valid_tempo_max)
    logger.debug("After fixing outliers:")
    logger.debug(df.describe())
    return df


@task
def train(
    train_data: pd.DataFrame,
    top_genres: list[str],
    impute_missing_values: bool = True,
    imputer_n_neighbors: int = 5,
    class_weight: str | None = "balanced",
    seed=42,
) -> tuple[Pipeline, MultiLabelBinarizer]:
    pipeline_steps = []

    ct = make_column_transformer(
        (MinMaxScaler(clip=True), NUMERICAL_COLS),
        ("passthrough", BINARY_COLS),
        remainder="drop",
    )
    pipeline_steps.append(ct)

    if impute_missing_values:
        imputer = KNNImputer(
            n_neighbors=imputer_n_neighbors, weights="distance"
        ).set_output(transform="pandas")
        pipeline_steps.append(imputer)

    rfc = RandomForestClassifier(random_state=seed, class_weight=class_weight)
    pipeline_steps.append(rfc)

    pipeline = make_pipeline(*pipeline_steps)
    mlb = MultiLabelBinarizer(classes=top_genres)

    X_train = train_data[FEATURE_COLS]
    y_train = mlb.fit_transform(train_data[LABEL_COL])

    pipeline = pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_train)

    _jaccard_score = jaccard_score(y_train, y_pred, average="samples")
    _hamming_loss = hamming_loss(y_train, y_pred)

    mlflow.sklearn.log_model(
        pipeline, "model", signature=infer_signature(X_train, y_pred)
    )
    mlflow.sklearn.log_model(
        mlb,
        "multi_label_binarizer",
        signature=infer_signature(train_data[LABEL_COL], y_train),
    )

    mlflow.log_metric("jaccard_score_train", _jaccard_score)
    mlflow.log_metric("hamming_loss_val", _hamming_loss)

    return pipeline, mlb


@task
def eval(
    test_data: pd.DataFrame,
    pipeline: Pipeline,
    mlb: MultiLabelBinarizer,
    register_model_if_accepted: bool,
    min_jaccard_score: float,
    max_hamming_loss: float,
) -> bool:
    logger = get_run_logger()
    X_test = test_data[FEATURE_COLS]
    y_true = mlb.transform(test_data[LABEL_COL])
    y_pred = pipeline.predict(X_test)
    _jaccard_score = jaccard_score(y_true, y_pred, average="samples")
    _hamming_loss = hamming_loss(y_true, y_pred)
    logger.info(f"jaccard score: {_jaccard_score:.4f}")
    logger.info(f"hamming loss: {_hamming_loss:.4f}")

    mlflow.log_metric("jaccard_score_val", _jaccard_score)
    mlflow.log_metric("hamming_loss_val", _hamming_loss)

    if not register_model_if_accepted:
        return False
    elif _jaccard_score >= min_jaccard_score and _hamming_loss <= max_hamming_loss:
        logger.info("Model evaluation criteria were met, registering model.")
        run = mlflow.active_run()

        new_version = mlflow.register_model(
            f"runs:/{run.info.run_id}/model",
            "genre-classifier-random-forest",
            tags={
                # In reality we'd do additional (possibly manual) checks before promoting to production
                "env": "production"
            },
        )
        logger.info(f"Registered classifier with version {new_version.version}")

        new_version = mlflow.register_model(
            f"runs:/{run.info.run_id}/multi_label_binarizer",
            "genre-classifier-multi-label-binarizer",
            tags={
                # In reality we'd do additional (possibly manual) checks before promoting to production
                "env": "production"
            },
        )
        logger.info(
            f"Registered MultiLabelBinarizer with version {new_version.version}"
        )
        return True
    else:
        logger.info(
            "Model evaluation criteria were NOT met. Model will not be registered."
        )
    return False


def log_params(**kwargs):
    mlflow.log_params(kwargs)


@flow(log_prints=True)
def train_flow(
    mlflow_experiment_name: str,
    bucket_block_name: str = "million-songs-dataset-s3",
    data_path: str = "subset",
    top_k_genres=50,
    valid_tempo_min: float = 70,
    valid_tempo_max: float = 180,
    impute_missing_values: bool = True,
    imputer_n_neighbors: int = 5,
    class_weight: str | None = "balanced",
    seed=42,
    register_model_if_accepted: bool = False,
    min_jaccard_score: float = 0.17,
    max_hamming_loss: float = 0.18,
):
    set_aws_credential_env()

    mlflow.set_tracking_uri("http://127.0.0.1:5000")
    mlflow.set_experiment(mlflow_experiment_name)

    log_params(
        top_k_genres=top_k_genres,
        valid_tempo_min=valid_tempo_min,
        valid_tempo_max=valid_tempo_max,
        impute_missing_values=impute_missing_values,
        imputer_n_neighbors=imputer_n_neighbors,
        class_weight=class_weight,
        seed=seed,
    )

    train_data = read_data(data_path + "/train.parquet", bucket_block_name)
    val_data = read_data(data_path + "/val.parquet", bucket_block_name)

    top_genres = get_top_genres(train_data, k=top_k_genres)

    train_data = filter_top_genres(train_data, top_genres)
    train_data = fix_outliers(train_data, valid_tempo_min, valid_tempo_max)

    val_data = filter_top_genres(val_data, top_genres)
    val_data = fix_outliers(val_data, valid_tempo_min, valid_tempo_max)

    trained_pipeline, mlb = train(
        train_data,
        top_genres,
        impute_missing_values=impute_missing_values,
        imputer_n_neighbors=imputer_n_neighbors,
        class_weight=class_weight,
        seed=seed,
    )

    eval(
        val_data,
        trained_pipeline,
        mlb,
        register_model_if_accepted=register_model_if_accepted,
        min_jaccard_score=min_jaccard_score,
        max_hamming_loss=max_hamming_loss,
    )


if __name__ == "__main__":
    train_flow("dev")
