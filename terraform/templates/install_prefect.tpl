#!/bin/bash

apt -y remove needrestart # Ubuntu 22.x has a feature where installations are interrupted by a dialog to restart/ update kernel. We remove this.
apt update -y && apt upgrade -y

apt-get install sqlite3 ca-certificates curl unzip gnupg python3-pip default-jdk -y

# Docker
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --yes --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo \deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable\ | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt update -y
apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y
apt-get install build-essential libpq-dev postgresql postgresql-contrib postgresql-client postgresql-client-common -y

# AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

docker -v
docker compose -v
systemctl start docker
usermod -aG docker $USER

aws --version

pip install -U prefect prefect-aws prefect-docker mlflow boto3 psycopg2-binary
prefect version

# Create a service file for Prefect Server and start & enable it
echo "${prefect_service}" > /etc/systemd/system/prefect-server.service
systemctl start prefect-server
systemctl enable prefect-server

sleep 60

# Create Docker worker pool
prefect work-pool create --type docker docker-work-pool --set-as-default

# Set concurrency limit of worker pool
prefect work-pool update --concurrency-limit 5 docker-work-pool

# Needs to be done before starting pool
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

# Create a service file for the docker work pool and start & enable it
echo "${work_pool_service}" > /etc/systemd/system/prefect-work-pool.service
systemctl start prefect-work-pool
systemctl enable prefect-work-pool

# Create a service file for the mlflow server and start & enable it
echo "${mlflow_server_service}" > /etc/systemd/system/mlflow-server.service
systemctl start mlflow-server
systemctl enable mlflow-server
