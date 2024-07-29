import datetime

from pathlib import Path
from prefect import flow, task, get_run_logger
from prefect_shell.commands import ShellOperation
from prefect.deployments import DeploymentImage


@task(retries=1, retry_delay_seconds=2)
def download_msd_subset(target_dir: Path) -> None:
    """Download the Million Song Dataset's subset used for development purposes.
    These are downloaded to the given target directory, and consist of many h5 files.
    """
    ShellOperation(
        commands=[
            "mkdir -p ${target_dir}",
            "wget -P ${target_dir}/ http://labrosa.ee.columbia.edu/~dpwe/tmp/millionsongsubset.tar.gz",
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
    for file in h5_files:
        logger.info(file)


@flow(log_prints=True)
def ingest_flow():
    download_completion = download_msd_subset(Path("data"))
    list_files(Path("data"), wait_for=[download_completion])


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
