# IaC setup

This document describes how to set up the environment using terraform in AWS.

## Prerequisites

### SSH Key

You need to have generated an ssh key pair and have the private key available on your machine.
The name should be `dev_key` and `dev_key.pub` for the private and public key respectively. 
The key can be generated using the following command:

```bash
cd ~/.ssh
ssh-keygen -P "" -t rsa -b 4096 -m pem -f dev_key
cat dev_key.pub
```

### `variables.tfvars`

Create a file named `variables.tfvars` in the `terraform` directory with the following content:

```hcl
dev_ssh_public_key = "ssh-rsa AAAAB3NzaC1y..."  # your public key
```

## Setup

1. Run `terraform init` to initialize the terraform environment.
2. Run `terraform apply -var-file=variables.tfvars` to create the resources.
3. Update ~/.ssh/config with the following content:

```ssh-config
Host prefect-zoomcamp
    HostName <IP>
    User ubuntu
    IdentityFile ~/.ssh/dev_key
    StrictHostKeyChecking no
```

4. Port forward the prefect server to your local machine by running `ssh -N -L 4200:localhost:4200 prefect-zoomcamp`.

After a couple of minutes should be able to access the Prefect UI through http://localhost:4200/dashboard.


## Caveats

- The prefect server uses SQLite as the default database. This is not recommended for production use, instead use PostgreSQL.