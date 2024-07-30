from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect_aws import S3Bucket

from urllib import request
import asyncio


from typing import Any, Optional
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


@task
def list_file_paths(bucket_folder: str, n: Optional[int] = None) -> list[str]:
    logger = get_run_logger()
    bucket = S3Bucket.load("million-songs-dataset-s3")
    objects = bucket.list_objects(folder=bucket_folder)
    object_keys = [obj["Key"] for obj in objects if obj["Key"].endswith(".h5")]
    logger.info(f"Found {len(object_keys)} objects")

    if n:
        return object_keys[:n]
    return object_keys


def normalize_genre(genre_str) -> str:
    return genre_str.lower().replace("-", " ")


@task(cache_key_fn=task_input_hash)
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


def get_features(h5_file: h5py.File) -> dict[str, float | int]:
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


@task
async def get_song_metadata(
    h5_path: str, genre_filter: list[str] = []
) -> dict[str, Any]:
    logger = get_run_logger()
    logger.info(f"Loading file from path {h5_path}")
    bucket = await S3Bucket.load("million-songs-dataset-s3")
    with BytesIO() as buf:
        await bucket.download_object_to_file_object(h5_path, buf)

        with h5py.File(buf, mode="r") as f:
            genre_tags = get_genres(f, genre_filter)
            features = get_features(f)

    metadata = {"features": features, "genre_tags": genre_tags}
    logger.info(metadata)
    return metadata


@flow(log_prints=True)
async def preprocess_flow(
    bucket_folder: str = "subset/MillionSongSubset",
    genres_url: str = DEFAULT_GENRES_URL,
    limit: Optional[int] = 20,
):
    paths = list_file_paths(bucket_folder, limit)
    genre_filter = get_genres_list(genres_url)
    coros = [get_song_metadata(path, genre_filter) for path in paths]
    song_metadata = await asyncio.gather(*coros)
    print(len(song_metadata), song_metadata[0])


if __name__ == "__main__":
    asyncio.run(preprocess_flow())
