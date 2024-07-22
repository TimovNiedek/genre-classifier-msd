#!/bin/bash

apt -y remove needrestart # Ubuntu 22.x has a feature where installations are interrupted by a dialog to restart/ update kernel. We remove this.
apt update -y && apt upgrade -y

apt-get install sqlite3 ca-certificates curl unzip gnupg python3-pip default-jdk -y

# AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

aws --version

pip install -U prefect
prefect version

# Create a service file for Prefect Server and start & enable it
echo "${prefect_service}" > /etc/systemd/system/prefect-server.service
systemctl start prefect-server
systemctl enable prefect-server
