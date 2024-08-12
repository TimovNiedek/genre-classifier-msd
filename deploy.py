from prefect import deploy
from prefect.deployments import DeploymentImage
import pendulum
from slugify import slugify

from genre_classifier.flows.ingest_data.flow import ingest_flow
from genre_classifier.flows.preprocess.flow import preprocess_flow
from genre_classifier.flows.split_data.flow import split_data_flow
from genre_classifier.flows.train.flow import train_flow
from genre_classifier.flows.predict.flow import predict_flow
from genre_classifier.flows.model_monitoring.flow import model_monitoring_flow
from genre_classifier.flows.complete_training.flow import complete_training_flow


VERSION = "v0"


if __name__ == "__main__":
    deploy(
        ingest_flow.to_deployment(name=f"genre-classifier-ingest-{VERSION}"),
        preprocess_flow.to_deployment(name=f"genre-classifier-preprocess-{VERSION}"),
        split_data_flow.to_deployment(name=f"genre-classifier-split-data-{VERSION}"),
        train_flow.to_deployment(name=f"genre-classifier-train-{VERSION}"),
        complete_training_flow.to_deployment(name=f"complete-training-{VERSION}"),
        model_monitoring_flow.to_deployment(
            # Generate a model monitoring report every morning at 6 AM.
            name=f"model-monitoring-{VERSION}",
            cron="0 6 * * *",
        ),
        predict_flow.to_deployment(
            # Execute a prediction every 5 minutes, in a real use-case this would be executed at the end of every day
            name=f"genre-classifier-predict-{VERSION}",
            cron="0/5 * * * *",
        ),
        work_pool_name="docker-work-pool",
        image=DeploymentImage(
            name="timovanniedek/genre-classifier-train",  # Change to your own repository on Docker Hub (must be public)
            tag=slugify(VERSION + "-" + pendulum.now().isoformat()),
            dockerfile="Dockerfile",
        ),
    )
