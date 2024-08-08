from prefect import flow, task, get_run_logger
from genre_classifier.utils import read_parquet_data
from sklearn.impute import KNNImputer
from sklearn.preprocessing import MinMaxScaler, MultiLabelBinarizer
from sklearn.compose import make_column_transformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import make_pipeline, Pipeline
from sklearn.metrics import jaccard_score, hamming_loss

import numpy as np
import pandas as pd


FEATURE_COLS = ["duration", "key", "loudness", "mode", "tempo", "year"]
NUMERICAL_COLS = ["duration", "loudness", "tempo", "year"]
BINARY_COLS = ["mode"]
CATEGORICAL_COLS = ["key"]
LABEL_COL = "genres"


@task
def read_data(data_path: str) -> pd.DataFrame:
    data = read_parquet_data(data_path, "million-songs-dataset-s3")
    return data


@task
def get_top_genres(df: pd.DataFrame, k=50) -> list[str]:
    genre_counts = df["genres"].explode().value_counts()
    genre_names = list(genre_counts.index)
    top_genre_names = genre_names[:k]
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
    df["year"] = df["year"].replace(0, np.nan)
    df.loc[df["tempo"] < valid_tempo_min / 2, "tempo"] = np.nan
    df.loc[df["tempo"] > valid_tempo_max * 2, "tempo"] = np.nan
    df.loc[df["tempo"] < valid_tempo_min, "tempo"] *= 2
    df.loc[df["tempo"] > valid_tempo_max, "tempo"] /= 2
    logger.debug("After fixing outliers:")
    logger.debug(df.describe())
    return df


@task
def train(
    train_data: pd.DataFrame, top_genres: list[str], seed=42
) -> tuple[Pipeline, MultiLabelBinarizer]:
    imputer = KNNImputer(n_neighbors=2, weights="distance").set_output(
        transform="pandas"
    )
    mlb = MultiLabelBinarizer(classes=top_genres)

    ct = make_column_transformer(
        (MinMaxScaler(clip=True), NUMERICAL_COLS),
        # (OneHotEncoder(), CATEGORICAL_COLS),
        ("passthrough", BINARY_COLS),
        remainder="drop",
    )

    rfc = RandomForestClassifier(random_state=seed, class_weight="balanced")

    pipeline = make_pipeline(imputer, ct, rfc)

    X_train = train_data[FEATURE_COLS]
    y_train = mlb.fit_transform(train_data[LABEL_COL])

    pipeline = pipeline.fit(X_train, y_train)
    return pipeline, mlb


@task
def eval(test_data: pd.DataFrame, pipeline: Pipeline, mlb: MultiLabelBinarizer):
    X_test = test_data[FEATURE_COLS]
    y_true = mlb.transform(test_data[LABEL_COL])
    y_pred = pipeline.predict(X_test)
    _jaccard_score = jaccard_score(y_true, y_pred, average="samples")
    _hamming_score = hamming_loss(y_true, y_pred)
    print(f"jaccard score: {_jaccard_score:.4f}")
    print(f"hamming loss: {_hamming_score:.4f}")


@flow(log_prints=True)
def train_flow(
    data_path: str = "subset",
    top_k_genres=50,
    valid_tempo_min: float = 70,
    valid_tempo_max: float = 180,
):
    train_data = read_data(data_path + "/train.parquet")
    val_data = read_data(data_path + "/val.parquet")

    top_genres = get_top_genres(train_data, k=top_k_genres)
    # TODO: artifact for top_genres

    train_data = filter_top_genres(train_data, top_genres)
    train_data = fix_outliers(train_data, valid_tempo_min, valid_tempo_max)

    val_data = filter_top_genres(val_data, top_genres)
    val_data = fix_outliers(val_data, valid_tempo_min, valid_tempo_max)

    # TODO: models for pipeline & mlb
    trained_pipeline, mlb = train(train_data, top_genres)
    eval(val_data, trained_pipeline, mlb)


if __name__ == "__main__":
    train_flow()
