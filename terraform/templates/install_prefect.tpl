#!/bin/bash

# TODO: You only need to do this if you created a separate EBS volume
# Mount external block and use that instead of the root block device
file -s /dev/nvme1n1
mkfs -t ext4 /dev/nvme1n1
mkdir /prefect-storage
mount /dev/nvme1n1 /prefect-storage/

cd /prefect-storage/

# ---

apt -y remove needrestart # Ubuntu 22.x has a feature where installations are interrupted by a dialog to restart/ update kernel. We remove this.
apt update -y && apt upgrade -y
apt-get install sqlite3 ca-certificates curl unzip gnupg python3-pip default-jdk -y

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

# TODO: This is only needed if you set up a different EBS volume
# Set Docker storage to the mounted volume using a symlink
systemctl stop docker
mv /var/lib/docker /prefect-storage/docker
ln -s /prefect-storage/docker /var/lib/docker

# ---

systemctl start docker

# Install Prefect
pip install prefect
pip install --upgrade jsonschema # I had to do this for the server to start
pip install prefect-docker

# Confirm installation
prefect version

# Spin up the server
prefect server start & # This is a blocking action. The following commands need to be run in separate terminals, hence the use of "&"

sleep 60 # Wait a couple seconds for server to start

# Create Docker worker pool
prefect work-pool create --type docker docker-work-pool --set-as-default

# Set concurrency limit of worker pool
prefect work-pool update --concurrency-limit 5 docker-work-pool

# Needs to be done before starting pool
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

# Start pool
prefect worker start --pool "docker-work-pool"

# Recommended
# Docker uses up storage very quickly (I noticed about 300MB extra was consumed after each flow run)
# You can set up a cron job to system prune hourly
(crontab -l ; echo "0 * * * * (sudo /usr/bin/docker system prune -f) 2>&1 | logger -t dockerPrune") | crontab - # Check logs using `grep 'dockerPrune' /var/log/syslog`