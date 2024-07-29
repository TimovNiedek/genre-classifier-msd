import datetime
from typing import Optional

from pathlib import Path
from prefect import flow, task, get_run_logger
from prefect_shell.commands import ShellOperation
from prefect.deployments import DeploymentImage
from prefect_aws import S3Bucket


@task(retries=1, retry_delay_seconds=2)
def download_msd_subset(target_dir: Path) -> None:
    """Download the Million Song Dataset's subset used for development purposes.
    These are downloaded to the given target directory, and consist of many h5 files.
    """
    ShellOperation(
        commands=[
            "mkdir -p ${target_dir}",
            "wget -q -P ${target_dir}/ http://labrosa.ee.columbia.edu/~dpwe/tmp/millionsongsubset.tar.gz",
            "tar -xzvf ${target_dir}/millionsongsubset.tar.gz -C ${target_dir}/",
        ],
        env={"target_dir": str(target_dir)},
    ).run()


@task
def list_files(data_dir: Path) -> None:
    """List the h5 files present in the data directory"""
    h5_files = list(sorted(Path(data_dir).rglob("*.h5")))
    logger = get_run_logger()
    logger.info(f"Total files: {len(h5_files)}")


@task
def upload_to_s3(data_dir: Path, target_dir: Optional[Path]) -> int:
    logger = get_run_logger()
    bucket = S3Bucket.load("million-songs-dataset-s3")
    to_path = str(target_dir) if target_dir is not None else None
    file_count = bucket.put_directory(local_path=str(data_dir), to_path=to_path)
    logger.info(f"Uploaded {file_count} files to {bucket.bucket_name}")
    return file_count


@flow(log_prints=True)
def ingest_flow():
    local_data_path = Path("data")
    download_completion = download_msd_subset(local_data_path)
    list_files(local_data_path, wait_for=[download_completion])
    upload_to_s3(local_data_path, Path("subset"), wait_for=[download_completion])


if __name__ == "__main__":
    ingest_flow.deploy(
        name="genre-classifier-ingest-v0",
        work_pool_name="docker-work-pool",
        image=DeploymentImage(
            name="timovanniedek/genre-classifier-ingest",
            tag=datetime.datetime.now().isoformat().replace(":", "-").lower(),
            dockerfile="Dockerfile",
        ),
    )
