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

### Terraform State bucket

Create an S3 bucket to store the terraform state. In [main.tf](./main.tf), update the following block with your bucket name:

```hcl
terraform {
  backend "s3" {
    bucket = "terraform-state-tvn"
    key    = "mlops-zoomcamp/state"
    region = "eu-central-1"
  }
}
```

## Setup

To create the infrastructure run `make infra` from the root directory. While the command will exit after the infrastructure is created,
the Prefect server will take around 5-10 minutes to start up.

If there are any errors, you may need to change the bucket names in [storage.tf](./storage.tf) and [create_s3_buckets.py](../genre_classifier/blocks/create_s3_buckets.py) to something unique.
These need to be unique across all AWS accounts, so you may need to add a prefix or suffix to the bucket names.

## Destroy

To destroy the infrastructure, run `make destroy` from the root directory.

## Additional notes

- The prefect server uses SQLite as the default database. This is not recommended for production use, instead use PostgreSQL.
- The current setup is not secure and should not be used in production.
- Prefect uses a docker work pool on the same machine as the server. Alternatively, an ECS work pool could be used to fully utilize the cloud resources.
