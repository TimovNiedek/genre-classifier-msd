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

### AWS authentication

Create an access key via the AWS console at [IAM > Security Credentials](https://us-east-1.console.aws.amazon.com/iam/home#/security_credentials).
Export these in your environment with:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

Also add them to the `variables.tfvars` file as described below.

### `variables.tfvars`

Create a file named `variables.tfvars` in the `terraform` directory with the following content:

```hcl
dev_ssh_public_key    = "ssh-rsa AAAAB3NzaC1y..."  # your public key
aws_access_key_id     = "..."  # AWS_ACCESS_KEY_ID
aws_access_key_secret = "..." # AWS_SECRET_ACCESS_KEY
```

## Setup

1. Run `terraform init` to initialize the terraform environment.
2. Run `terraform apply -var-file=variables.tfvars` to create the resources.

## Additional notes

- The prefect server uses SQLite as the default database. This is not recommended for production use, instead use PostgreSQL.
- The current setup is not secure and should not be used in production.
- Prefect uses a docker work pool on the same machine as the server. Alternatively, an ECS work pool could be used to fully utilize the cloud resources.
