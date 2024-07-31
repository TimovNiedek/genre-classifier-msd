from prefect import flow, task, get_run_logger
from prefect_aws import S3Bucket
from prefect.task_runners import ConcurrentTaskRunner
from pydantic import BaseModel
from pathlib import Path
import pandas as pd
import tempfile

from urllib import request

from typing import Optional
import h5py
from io import BytesIO


DEFAULT_GENRES_URL = "https://gist.githubusercontent.com/andytlr/4104c667a62d8145aa3a/raw/2d044152bcacf98d401b71df2cb67fade8e490c9/spotify-genres.md"
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


bucket = S3Bucket.load("million-songs-dataset-s3")


@task
def list_file_paths(bucket_folder: str, n: Optional[int] = None) -> list[str]:
    logger = get_run_logger()
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
    for line in genre_lines[4:]:
        genre = line.strip()[3:]
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


def get_song_metadata(h5_path: str, genre_filter: list[str] = []) -> SongMetadata:
    with BytesIO() as buf:
        bucket.download_object_to_file_object(h5_path, buf)

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
):
    logger = get_run_logger()
    df = pd.DataFrame([dict(song) for song in song_metadata])
    logger.info(df.head())

    with tempfile.NamedTemporaryFile(mode="w") as f:
        df.to_parquet(f.name)
        bucket.upload_from_path(from_path=f.name, to_path=target_path)


@task
def get_song_metadata_list(
    h5_paths: str, genre_filter: list[str] = []
) -> list[SongMetadata]:
    song_metas = []
    for h5_path in h5_paths:
        song_metadata = get_song_metadata(h5_path, genre_filter=genre_filter)
        song_metas.append(song_metadata)
    return song_metas


@flow(task_runner=ConcurrentTaskRunner())
def preprocess_flow(
    bucket_folder: str = "subset/MillionSongSubset",
    genres_url: str = DEFAULT_GENRES_URL,
    limit: Optional[int] = 10,
):
    logger = get_run_logger()
    paths = list_file_paths.submit(bucket_folder, limit)
    genre_filter = get_genres_list.submit(genres_url)
    song_metas = get_song_metadata_list(paths, genre_filter)
    logger.info(f"Extracted metadata for {len(song_metas)} songs")
    write_features(song_metas, target_path="subset/MillionSongSubset/subset.parquet")


if __name__ == "__main__":
    preprocess_flow.deploy(
        name="genre-classifier-preprocess-v0",
        work_pool_name="docker-work-pool",
        image="timovanniedek/genre-classifier-preprocess",
    )
