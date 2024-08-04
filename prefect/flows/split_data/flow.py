from prefect import flow, task
from prefect_aws import S3Bucket
from sklearn.model_selection import train_test_split


import pandas as pd

bucket = S3Bucket.load("million-songs-dataset-s3")


@task
def read_data(data_path: str) -> pd.DataFrame:
    full_path = "s3://" + "/".join([bucket.bucket_name, data_path])
    data = pd.read_parquet(full_path).set_index("song_id")
    return data


@task
def split_train_val_test(
    df: pd.DataFrame, val_size, test_size, seed: int = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train_data, val_test_data = train_test_split(
        df, test_size=val_size + test_size, random_state=seed
    )
    val_data, test_data = train_test_split(
        val_test_data, test_size=test_size / (val_size + test_size), random_state=seed
    )
    return train_data, val_data, test_data


@task
def upload_df_to_s3(df: pd.DataFrame, data_path: str) -> None:
    full_path = "s3://" + "/".join([bucket.bucket_name, data_path])
    df.to_parquet(full_path)


@flow(log_prints=True)
def split_data(
    data_path: str = "subset/MillionSongSubset/subset.parquet",
    val_size: float = 0.1,
    test_size: float = 0.1,
    seed: int | None = 42,
):
    full_data = read_data(data_path)
    train_data, val_data, test_data = split_train_val_test(
        full_data, val_size, test_size, seed=seed
    )
    upload_df_to_s3(train_data, "subset/train.parquet")
    upload_df_to_s3(val_data, "subset/val.parquet")
    upload_df_to_s3(test_data, "subset/test.parquet")


if __name__ == "__main__":
    split_data()
