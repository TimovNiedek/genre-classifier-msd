import datetime

from prefect import flow, task

current_date = datetime.date.today()


@task
def print_current_date():
    global current_date
    current_date = current_date + datetime.timedelta(days=1)
    print(current_date)


@flow(log_prints=True)
def predict_flow():
    print_current_date()


if __name__ == "__main__":
    print_current_date()
