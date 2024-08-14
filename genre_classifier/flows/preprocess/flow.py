import asyncio
import tempfile
from io import BytesIO
from pathlib import Path
from typing import Optional
from urllib import request

import h5py
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_aws import S3Bucket
from pydantic import BaseModel

DEFAULT_GENRES_URL = "https://gist.githubusercontent.com/TimovNiedek/0530d9bc36aa3b3e83df4714c9a68c86/raw/5c7d92f81ed2f78ea949238c7563af0626d43b7d/spotify-genres.txt"
ANALYSIS_FEATURE_NAMES = [
    "danceability",
    "duration",
    "energy",
    "key",
    "loudness",
    "mode",
    "tempo",
]
MUSICBRAINZ_FEATURE_NAMES = ["year"]


@task
def list_file_paths(
    bucket_folder: str,
    n: Optional[int] = None,
    bucket_block_name: str = "million-songs-dataset-s3",
) -> list[str]:
    logger = get_run_logger()
    bucket = S3Bucket.load(bucket_block_name)
    objects = bucket.list_objects(folder=bucket_folder)
    object_keys = [obj["Key"] for obj in objects if obj["Key"].endswith(".h5")]
    logger.info(f"Found {len(object_keys)} objects")

    if n:
        return object_keys[:n]
    return object_keys


def normalize_genre(genre_str) -> str:
    return genre_str.lower().replace("-", " ")


@task
def get_genres_list(url: str = DEFAULT_GENRES_URL) -> list[str]:
    logger = get_run_logger()
    with request.urlopen(url) as f:
        genre_lines = f.read().decode().split("\n")

    genres_list = []
    for line in genre_lines:
        genre = line.strip()
        genres_list.append(normalize_genre(genre))

    logger.info(f"Got {len(genres_list)} genres")
    logger.info(f"Sample: {genres_list[:5]}")

    return genres_list


def get_features(h5_file: h5py.File) -> dict[str, float | int | None]:
    features = {}
    for feature_name in ANALYSIS_FEATURE_NAMES:
        feature_value = h5_file["analysis"]["songs"][feature_name][0]
        features[feature_name] = feature_value
    for feature_name in MUSICBRAINZ_FEATURE_NAMES:
        feature_value = h5_file["musicbrainz"]["songs"][feature_name][0]
        features[feature_name] = feature_value
    return features


def get_genres(h5_file: h5py.File, genre_filter: list[str]) -> list[str]:
    tags = [s.decode("UTF-8") for s in h5_file["metadata"]["artist_terms"][()]]
    genre_tags = [normalize_genre(tag) for tag in tags]
    genre_tags = [tag for tag in genre_tags if tag in genre_filter]
    return genre_tags


class SongMetadata(BaseModel):
    song_id: str
    danceability: float | None
    duration: float | None
    energy: float | None
    key: int | None
    loudness: float | None
    mode: int | None
    tempo: float | None
    year: int | None
    genres: list[str]


def get_song_metadata(
    h5_path: str,
    genre_filter: list[str] = [],
    bucket_block_name: str = "million-songs-dataset-s3",
) -> SongMetadata:
    bucket = S3Bucket.load(bucket_block_name)
    with BytesIO() as buf:
        bucket.download_object_to_file_object(h5_path, buf)

        with h5py.File(buf, mode="r") as f:
            genre_tags = get_genres(f, genre_filter)
            features = get_features(f)

    song_id = Path(h5_path).stem
    song_metadata = SongMetadata(song_id=song_id, genres=genre_tags, **features)

    return song_metadata


async def get_song_metadata_async(
    h5_path: str,
    bucket_block: S3Bucket,
    genre_filter: list[str] = [],
) -> SongMetadata:
    with BytesIO() as buf:
        await bucket_block.download_object_to_file_object(h5_path, buf)

        with h5py.File(buf, mode="r") as f:
            genre_tags = get_genres(f, genre_filter)
            features = get_features(f)

    song_id = Path(h5_path).stem
    song_metadata = SongMetadata(song_id=song_id, genres=genre_tags, **features)

    return song_metadata


@task
def write_features(
    song_metadata: list[SongMetadata],
    target_path: str = "subset/MillionSongSubset/subset.parquet",
    bucket_block_name: str = "million-songs-dataset-s3",
):
    logger = get_run_logger()
    bucket = S3Bucket.load(bucket_block_name)
    df = pd.DataFrame([dict(song) for song in song_metadata])
    logger.info(df.head())

    with tempfile.NamedTemporaryFile(mode="w") as f:
        df.to_parquet(f.name)
        bucket.upload_from_path(from_path=f.name, to_path=target_path)


@task
async def get_song_metadata_list(
    h5_paths: str,
    genre_filter: list[str] = [],
    bucket_block_name: str = "million-songs-dataset-s3",
) -> list[SongMetadata]:
    song_metas = []
    bucket_block = await S3Bucket.load(bucket_block_name)
    for h5_path in h5_paths:
        song_metadata = await get_song_metadata_async(
            h5_path, genre_filter=genre_filter, bucket_block=bucket_block
        )
        song_metas.append(song_metadata)
    return song_metas


@flow(task_runner=ConcurrentTaskRunner())
async def preprocess_flow(
    bucket_folder: str = "subset/MillionSongSubset",
    target_path: str = "subset/MillionSongSubset/subset.parquet",
    s3_bucket_block_name: str = "million-songs-dataset-s3",
    genres_url: str = DEFAULT_GENRES_URL,
    limit: Optional[int] = None,
) -> str:
    """Preprocess the Million Song Dataset.

    Args:
        bucket_folder (str): The folder in the S3 bucket where the dataset is stored.
        target_path (str): The path where the preprocessed data will be stored.
        s3_bucket_block_name (str): The name of the S3 bucket block in Prefect.
        genres_url (str): The URL to the list of genres.
        limit (int): The number of songs to process.

    Returns:
        str: The path to the preprocessed data relative to the S3 bucket.
    """
    paths = list_file_paths.submit(bucket_folder, limit, s3_bucket_block_name)
    genre_filter = get_genres_list.submit(genres_url)
    song_metas = await get_song_metadata_list(
        paths, genre_filter, bucket_block_name=s3_bucket_block_name
    )
    write_features(
        song_metas, target_path=target_path, bucket_block_name=s3_bucket_block_name
    )
    return target_path


if __name__ == "__main__":
    asyncio.run(
        preprocess_flow(
            target_path="subset/MillionSongSubset/subset-test.parquet", limit=10
        )
    )
