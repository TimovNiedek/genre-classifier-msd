# genre-classifier-msd

## Getting started

### Prerequisites

* [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
* AWS account
* AWS credentials should be available in environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, following the [AWS Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/aws-build) instructions.
* Poetry should be installed, following the [installation instructions](https://python-poetry.org/docs/#installation).
* An account on [docker hub](https://hub.docker.com/) - this can be created for free if you do not have one yet. Create a public repository, for example `yourusername/genre-classifier-train`. This is necessary to be able to deploy the flow by building a docker image and pulling it on the Prefect server.

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

### Initialize local environment

To install the dependencies locally, run `make init`. This simply instructs poetry to use python 3.12 and installs packages from the poetry.lock file.
If you prefer to use a different environment manager, you can install from `requirements.txt` directly.

## Deploy

Update the `name` value inside the `DeploymentImage` at [deploy.py](./deploy.py) to point to your public repository. Otherwise you will not be able to build the image & pull it from the Prefect server.

To deploy the training & inference code, run `make deploy`. This will connect to the Prefect server, create the required AWS blocks, build the Docker container and deploy the Prefect flows.

If everything is successful, you should see the following output:

```
Successfully created/updated all deployments!

                             Deployments
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┓
┃ Name                                           ┃ Status  ┃ Details ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━┩
│ ingest-flow/genre-classifier-ingest-v0         │ applied │         │
├────────────────────────────────────────────────┼─────────┼─────────┤
│ preprocess-flow/genre-classifier-preprocess-v0 │ applied │         │
├────────────────────────────────────────────────┼─────────┼─────────┤
│ split-data-flow/genre-classifier-split-data-v0 │ applied │         │
├────────────────────────────────────────────────┼─────────┼─────────┤
│ train-flow/genre-classifier-train-v0           │ applied │         │
├────────────────────────────────────────────────┼─────────┼─────────┤
│ predict-flow/genre-classifier-predict-v0       │ applied │         │
└────────────────────────────────────────────────┴─────────┴─────────┘
```

## To-Do's

* [x] Add makefile
    * [x] Deploy training pipeline
    * [x] Execute training pipeline
    * [ ] Test flow steps
* [ ] Add unit tests
* [ ] Add integration tests
* [ ] Add CI / CD
* [x] Add mlflow server to IaC
* [ ] Add observability tooling
* [x] Add experiment tracking
* [x] Design deployment method (batch / streaming)
    * [x] Mock new incoming data
* [x] Containerize & deploy model
* [x] Ensure dependency versions are specified
* [ ] Experiment with better model architectures
