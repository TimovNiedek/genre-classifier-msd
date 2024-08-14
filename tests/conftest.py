import pytest
from prefect.testing.utilities import prefect_test_harness

from genre_classifier.blocks.create_aws_credentials import create_aws_creds_block
from genre_classifier.blocks.create_s3_buckets import create_s3_buckets


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        create_aws_creds_block()
        create_s3_buckets()
        yield
