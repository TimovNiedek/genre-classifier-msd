import asyncio

from prefect import flow

from genre_classifier.flows.ingest_data.flow import ingest_flow
from genre_classifier.flows.preprocess.flow import preprocess_flow
from genre_classifier.flows.split_data.flow import split_data_flow
from genre_classifier.flows.train.flow import train_flow


@flow
def complete_training_flow(
    mlflow_experiment_name: str,
    bucket_block_name: str = "million-songs-dataset-s3",
    songs_dataset_size_limit: int | None = None,
    val_size: float = 0.1,
    test_size: float = 0.1,
    top_k_genres=50,
    valid_tempo_min: float = 70,
    valid_tempo_max: float = 180,
    impute_missing_values: bool = True,
    imputer_n_neighbors: int = 5,
    class_weight: str | None = "balanced",
    seed=42,
    register_model_if_accepted: bool = True,
    min_jaccard_score: float = 0.17,
    max_hamming_loss: float = 0.18,
):
    ingested_data_path = ingest_flow()
    preprocessed_data_path = asyncio.run(
        preprocess_flow(
            bucket_folder=ingested_data_path,
            s3_bucket_block_name=bucket_block_name,
            limit=songs_dataset_size_limit,
        )
    )
    split_data_path = split_data_flow(
        bucket_block_name=bucket_block_name,
        source_data_path=preprocessed_data_path,
        val_size=val_size,
        test_size=test_size,
        seed=seed,
    )
    train_flow(
        mlflow_experiment_name,
        bucket_block_name=bucket_block_name,
        data_path=split_data_path,
        top_k_genres=top_k_genres,
        valid_tempo_min=valid_tempo_min,
        valid_tempo_max=valid_tempo_max,
        impute_missing_values=impute_missing_values,
        imputer_n_neighbors=imputer_n_neighbors,
        class_weight=class_weight,
        seed=seed,
        register_model_if_accepted=register_model_if_accepted,
        min_jaccard_score=min_jaccard_score,
        max_hamming_loss=max_hamming_loss,
    )


if __name__ == "__main__":
    complete_training_flow()
