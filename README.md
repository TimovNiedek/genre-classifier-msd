# genre-classifier-msd

## Getting started

### Prerequisites

* [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
* AWS account
* AWS credentials should be available in environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, following the [AWS Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/aws-build) instructions.

### Set up infrastructure

Follow the instructions at [terraform/README.md](terraform/README.md) to set up the infrastructure using Terraform.

### Connecting to Prefect

1. Update ~/.ssh/config with the following content:
    ```ssh-config
    Host prefect-zoomcamp
        HostName <IP>
        User ubuntu
        IdentityFile ~/.ssh/dev_key
        StrictHostKeyChecking no
    ```
2. Port forward the prefect server to your local machine by running `ssh -N -L 4200:localhost:4200 prefect-zoomcamp`.
3. After a couple of minutes should be able to access the Prefect UI through http://localhost:4200/dashboard.
4. Run `pipenv run prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"` before executing flows.
5. Set the default work pool to the docker work pool: `prefect config set PREFECT_DEFAULT_WORK_POOL_NAME=docker-work-pool`

## To-Do's

* [ ] Add makefile
    * [ ] Deploy training pipeline
    * [ ] Execute training pipeline
    * [ ] Test flow steps
* [ ] Add unit tests
* [ ] Add integration tests
* [ ] Add CI / CD
* [ ] Add mlflow server to IaC
* [ ] Add observability tooling
* [ ] Add experiment tracking
* [ ] Design deployment method (batch / streaming)
    * [ ] Mock new incoming data
* [ ] Containerize & deploy model
* [ ] Ensure dependency versions are specified
* [ ] Experiment with better model architectures
