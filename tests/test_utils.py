import os
from unittest.mock import MagicMock, patch

import pandas as pd

from genre_classifier.utils import (
    download_file_from_s3,
    get_file_uri,
    read_parquet_data,
    set_aws_credential_env,
    upload_dir_to_s3,
    upload_file_to_s3,
    write_parquet_data,
)


class TestUtils:
    @patch("genre_classifier.utils.AwsCredentials.load")
    def test_set_aws_credential_env(self, mock_load):
        mock_creds = MagicMock()
        mock_creds.aws_access_key_id = "test_access_key"
        mock_creds.aws_secret_access_key.get_secret_value.return_value = (
            "test_secret_key"
        )
        mock_load.return_value = mock_creds

        set_aws_credential_env("aws-creds")

        assert os.environ["AWS_ACCESS_KEY_ID"] == "test_access_key"
        assert os.environ["AWS_SECRET_ACCESS_KEY"] == "test_secret_key"

    @patch("genre_classifier.utils.S3Bucket.load")
    def test_get_file_uri(self, mock_load):
        mock_bucket = MagicMock()
        mock_bucket.bucket_name = "test-bucket"
        mock_load.return_value = mock_bucket

        uri = get_file_uri("data/file.txt", "million-songs-dataset-s3")
        assert uri == "s3://test-bucket/data/file.txt"

    @patch("genre_classifier.utils.S3Bucket.load")
    def test_upload_dir_to_s3(self, mock_load):
        mock_bucket = MagicMock()
        mock_load.return_value = mock_bucket
        mock_bucket.put_directory.return_value = 5

        file_count = upload_dir_to_s3(
            "data/dir", "target/dir", "million-songs-dataset-s3"
        )
        assert file_count == 5
        mock_bucket.put_directory.assert_called_once_with(
            local_path="data/dir", to_path="target/dir"
        )

    @patch("genre_classifier.utils.S3Bucket.load")
    def test_upload_file_to_s3(self, mock_load):
        mock_bucket = MagicMock()
        mock_load.return_value = mock_bucket

        upload_file_to_s3(
            "data/file.txt", "target/file.txt", "million-songs-dataset-s3"
        )
        mock_bucket.upload_from_path.assert_called_once_with(
            "data/file.txt", "target/file.txt"
        )

    @patch("genre_classifier.utils.S3Bucket.load")
    def test_download_file_from_s3(self, mock_load):
        mock_bucket = MagicMock()
        mock_load.return_value = mock_bucket

        download_file_from_s3(
            "data/file.txt", "target/file.txt", "million-songs-dataset-s3"
        )
        mock_bucket.download_object_to_path.assert_called_once_with(
            "data/file.txt", "target/file.txt"
        )

    @patch("genre_classifier.utils.S3Bucket.load")
    @patch("pandas.read_parquet")
    @patch("tempfile.NamedTemporaryFile")
    def test_read_parquet_data(self, mock_tempfile, mock_read_parquet, mock_load):
        mock_bucket = MagicMock()
        mock_load.return_value = mock_bucket
        mock_tempfile.return_value.__enter__.return_value.name = "tempfile"
        mock_read_parquet.return_value = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        df = read_parquet_data("data/file.parquet", "million-songs-dataset-s3")
        mock_bucket.download_object_to_path.assert_called_once_with(
            "data/file.parquet", "tempfile"
        )
        mock_read_parquet.assert_called_once_with("tempfile")
        assert df.equals(pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}))

    @patch("genre_classifier.utils.S3Bucket.load")
    @patch("pandas.DataFrame.to_parquet")
    @patch("tempfile.NamedTemporaryFile")
    def test_write_parquet_data(self, mock_tempfile, mock_to_parquet, mock_load):
        mock_bucket = MagicMock()
        mock_load.return_value = mock_bucket
        mock_tempfile.return_value.__enter__.return_value.name = "tempfile"

        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        write_parquet_data(df, "target/file.parquet", "million-songs-dataset-s3")
        mock_to_parquet.assert_called_once_with("tempfile")
        mock_bucket.upload_from_path.assert_called_once_with(
            "tempfile", "target/file.parquet"
        )
