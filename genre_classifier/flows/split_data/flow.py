from prefect import flow, task
from sklearn.model_selection import train_test_split

from genre_classifier.utils import read_parquet_data, write_parquet_data

import pandas as pd
import datetime


@task
def read_data(data_path: str) -> pd.DataFrame:
    data = read_parquet_data(data_path, "million-songs-dataset-s3").set_index("song_id")
    return data


@task
def split_by_release_year(
    df: pd.DataFrame, test_size: float
) -> tuple[pd.DataFrame, pd.DataFrame]:
    sorted_df = df.sort_values(by="year", axis="index")

    cutoff_index = int(len(sorted_df) * (1 - test_size))
    return sorted_df.iloc[:cutoff_index], sorted_df.iloc[cutoff_index:]


@task
def random_split(
    df: pd.DataFrame, test_size, seed: int = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train_data, test_data = train_test_split(df, test_size=test_size, random_state=seed)
    return train_data, test_data


@task
def upload_df_to_s3(
    df: pd.DataFrame,
    data_path: str,
    bucket_block_name: str = "million-songs-dataset-s3",
) -> None:
    write_parquet_data(df, data_path, bucket_block_name)


@task
def add_daily_releases(
    df: pd.DataFrame,
    start_date: datetime.date,
    num_tracks_per_day: int,
    target_bucket_block_name: str = "million-songs-dataset-s3",
    target_data_path: str = "daily",
):
    df = df.sort_values(by="year", axis="index")
    current_date = start_date
    for slice_idx in range(0, len(df), num_tracks_per_day):
        chunk = df.iloc[slice_idx : slice_idx + num_tracks_per_day]
        write_parquet_data(
            chunk,
            f"{target_data_path}/{current_date}/releases.parquet",
            bucket_block_name=target_bucket_block_name,
        )
        current_date = current_date + datetime.timedelta(days=1)


@flow(log_prints=True)
def split_data_flow(
    bucket_block_name: str = "million-songs-dataset-s3",
    source_data_path: str = "subset/MillionSongSubset/subset.parquet",
    target_data_path: str = "subset",
    val_size: float = 0.1,
    test_size: float = 0.1,
    seed: int | None = 42,
    new_releases_start_date: datetime.date = datetime.date.today(),
    num_releases_per_day: int = 10,
):
    full_data = read_data(source_data_path)
    train_val_set, test_set = split_by_release_year(full_data, test_size=test_size)
    train_set, val_set = random_split(
        train_val_set, val_size / (1 - test_size), seed=seed
    )

    upload_df_to_s3(train_set, f"{target_data_path}/train.parquet", bucket_block_name)
    upload_df_to_s3(val_set, f"{target_data_path}/val.parquet", bucket_block_name)
    add_daily_releases(
        test_set,
        new_releases_start_date,
        num_releases_per_day,
        bucket_block_name,
        f"{target_data_path}/daily",
    )


if __name__ == "__main__":
    split_data_flow()
