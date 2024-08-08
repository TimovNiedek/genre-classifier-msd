from prefect import deploy
from prefect.deployments import DeploymentImage
import pendulum
from slugify import slugify

from genre_classifier.blocks.create_aws_credentials import create_aws_creds_block
from genre_classifier.blocks.create_s3_buckets import create_s3_buckets

from genre_classifier.flows.ingest_data.flow import ingest_flow
from genre_classifier.flows.preprocess.flow import preprocess_flow
from genre_classifier.flows.split_data.flow import split_data_flow
from genre_classifier.flows.train.flow import train_flow

import time


VERSION = "v0"


if __name__ == "__main__":
    create_aws_creds_block()
    time.sleep(0.5)  # creating the block is async, but required for the s3 bucket
    create_s3_buckets()

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
