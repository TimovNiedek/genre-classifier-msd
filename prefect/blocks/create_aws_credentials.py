import os

from prefect_aws import AwsCredentials


def create_aws_creds_block():
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if aws_access_key_id is None:
        raise ValueError("Missing AWS_ACCESS_KEY_ID environment variable")

    if aws_secret_access_key is None:
        raise ValueError("Missing AWS_SECRET_ACCESS_KEY environment variable")

    aws_credentials_block = AwsCredentials(
        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
    )

    aws_credentials_block.save(name="aws-creds", overwrite=True)


if __name__ == "__main__":
    create_aws_creds_block()
