from prefect import flow, task, get_run_logger
from genre_classifier.utils import read_parquet_data

import numpy as np
import pandas as pd


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
    logger.info("Before fixing outliers:")
    logger.info(df.describe())
    df["year"] = df["year"].replace(0, np.nan)
    df.loc[df["tempo"] < valid_tempo_min / 2, "tempo"] = np.nan
    df.loc[df["tempo"] > valid_tempo_max * 2, "tempo"] = np.nan
    df.loc[df["tempo"] < valid_tempo_min, "tempo"] *= 2
    df.loc[df["tempo"] > valid_tempo_max, "tempo"] /= 2
    logger.info("After fixing outliers:")
    logger.info(df.describe())
    return df


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

    train_data = filter_top_genres(train_data, top_genres)
    train_data = fix_outliers(train_data, valid_tempo_min, valid_tempo_max)

    val_data = filter_top_genres(val_data, top_genres)
    val_data = fix_outliers(train_data, valid_tempo_min, valid_tempo_max)


if __name__ == "__main__":
    train_flow()
