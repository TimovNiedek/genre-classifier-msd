from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger, task
from prefect_aws import S3Bucket
from prefect_shell.commands import ShellOperation


@task(retries=1, retry_delay_seconds=2)
def download_msd_subset(
    target_dir: Path,
    url: str = "http://labrosa.ee.columbia.edu/~dpwe/tmp/millionsongsubset.tar.gz",
) -> None:
    """Download the Million Song Dataset's subset used for development purposes.
    These are downloaded to the given target directory, and consist of many h5 files.
    """
    ShellOperation(
        commands=[
            f"mkdir -p {target_dir}",
            f"wget -q -P {target_dir}/ {url}",
            f'tar -xzf {target_dir}/millionsongsubset.tar.gz -C {target_dir}/ --exclude="._*"',
        ],
    ).run()


@task
def list_files(data_dir: Path) -> None:
    """List the h5 files present in the data directory"""
    h5_files = list(sorted(Path(data_dir).rglob("*.h5")))
    logger = get_run_logger()
    logger.info(f"Total files: {len(h5_files)}")


@task
def upload_to_s3(
    data_dir: Path,
    target_dir: Optional[Path],
    bucket_block_name: str = "million-songs-dataset-s3",
) -> int:
    logger = get_run_logger()
    bucket = S3Bucket.load(bucket_block_name)
    to_path = str(target_dir) if target_dir is not None else None
    file_count = bucket.put_directory(local_path=str(data_dir), to_path=to_path)
    logger.info(f"Uploaded {file_count} files to {bucket.bucket_name}")
    return file_count


@flow(log_prints=True)
def ingest_flow() -> str:
    local_data_path = Path("data")
    download_completion = download_msd_subset(local_data_path)
    list_files(local_data_path, wait_for=[download_completion])
    upload_to_s3(local_data_path, Path("subset"), wait_for=[download_completion])
    return "subset/MillionSongSubset"


if __name__ == "__main__":
    ingest_flow()
