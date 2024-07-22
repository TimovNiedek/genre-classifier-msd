# IaC setup

This document describes how to set up the Prefect server using terraform in AWS.
This is largely based on [this guide](https://medium.com/@kelvingakuo/self-hosting-prefect-on-aws-ec2-managed-via-terraform-and-prefect-yaml-53f2795f6e4c),
with several modifications.

My goal was to set up the Prefect server in a way that would easily facilitate collaboration through a shared EC2 instance,
and to make it easy to set up and tear down the server as needed. For additional stability, I've modified the Terraform code from the 
blog post to start the Prefect server and work pool as a systemd service, so that it will automatically restart if the server is rebooted.

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
5. After a couple of minutes should be able to access the Prefect UI through http://localhost:4200/dashboard.
6. Run `pipenv run prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"` before executing flows.

## Additional notes

- The prefect server uses SQLite as the default database. This is not recommended for production use, instead use PostgreSQL.
- The current setup is not secure and should not be used in production.
- Prefect uses a docker work pool on the same machine as the server. Alternatively, an ECS work pool could be used to fully utilize the cloud resources. 