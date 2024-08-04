FROM prefecthq/prefect:2.19.9-python3.12

RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/prefect/ingest_data/requirements.txt
RUN python -m pip install -r /opt/prefect/ingest_data/requirements.txt

COPY . /opt/prefect/ingest_data/
WORKDIR /opt/prefect/ingest_data/
