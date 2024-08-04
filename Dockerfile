FROM prefecthq/prefect:2.19.9-python3.12

RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/prefect/requirements.txt
RUN python -m pip install -r /opt/prefect/requirements.txt

COPY genre_classifier /opt/prefect/genre_classifier
WORKDIR /opt/prefect
