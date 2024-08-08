from prefect import deploy
from prefect.deployments import DeploymentImage
import pendulum
from slugify import slugify

from genre_classifier.flows.ingest_data.flow import ingest_flow
from genre_classifier.flows.preprocess.flow import preprocess_flow
from genre_classifier.flows.split_data.flow import split_data_flow
from genre_classifier.flows.train.flow import train_flow


VERSION = "v0"


if __name__ == "__main__":
    deploy(
        ingest_flow.to_deployment(name=f"genre-classifier-ingest-{VERSION}"),
        preprocess_flow.to_deployment(name=f"genre-classifier-preprocess-{VERSION}"),
        split_data_flow.to_deployment(name=f"genre-classifier-split-data-{VERSION}"),
        train_flow.to_deployment(name=f"genre-classifier-train-{VERSION}"),
        work_pool_name="docker-work-pool",
        image=DeploymentImage(
            name="timovanniedek/genre-classifier-train",
            tag=slugify(VERSION + "-" + pendulum.now().isoformat()),
            dockerfile="Dockerfile",
        ),
    )
