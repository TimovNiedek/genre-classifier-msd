import datetime

from prefect import flow, task
from prefect import runtime


@task
def print_current_date(start_date):
    current_date = start_date + datetime.timedelta(days=runtime.flow_run.run_count - 1)
    print(current_date)


@flow(log_prints=True)
def predict_flow(start_date=datetime.date.today()):
    print_current_date(start_date)


if __name__ == "__main__":
    print_current_date()
