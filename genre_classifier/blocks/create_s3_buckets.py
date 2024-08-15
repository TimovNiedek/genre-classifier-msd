from prefect_aws import AwsCredentials, S3Bucket


def create_s3_buckets():
    aws_credentials_block = AwsCredentials.load("aws-creds")
    s3_bucket_block = S3Bucket(
        bucket_name="million-songs-dataset",
        credentials=aws_credentials_block,
    )
    s3_bucket_block.save(name="million-songs-dataset-s3", overwrite=True)

    cicd_bucket_block = S3Bucket(
        bucket_name="million-songs-dataset-cicd",
        credentials=aws_credentials_block,
    )
    cicd_bucket_block.save(name="million-songs-dataset-s3-cicd", overwrite=True)

    cicd_bucket_block = S3Bucket(
        bucket_name="evidently-static-dashboard-tvn",
        credentials=aws_credentials_block,
    )
    cicd_bucket_block.save(name="evidently-static-dashboard", overwrite=True)


if __name__ == "__main__":
    create_s3_buckets()
