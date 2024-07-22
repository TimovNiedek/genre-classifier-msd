from prefect import flow, task
import pandas as pd


@task
def ingest_file(color="yellow", year=2024, month=3) -> pd.DataFrame:
    df = pd.read_parquet(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"
    )
    return df


@task
def print_size(data: pd.DataFrame):
    print(len(data))


@flow(log_prints=True)
def basic_flow(color="yellow", year=2024, month=3):
    data = ingest_file(color, year, month)
    print_size(data)


if __name__ == "__main__":
    basic_flow()
