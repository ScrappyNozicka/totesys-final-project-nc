import pytest
from moto import mock_aws
import boto3
import os
import pandas as pd

from src.transform.transform_utils.processed_s3_handler import (
    ProcessedS3Handler,
)


@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""

    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function", autouse=True)
def mock_aws_setup():
    """
    Setup a mock AWS S3 bucket using moto's
    mock_aws in the eu-west-2 region.
    """
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # Set environment variable for bucket name (normally loaded from .env)
        os.environ["PROCESSED_S3_BUCKET_NAME"] = bucket_name

        yield s3_client


@pytest.fixture
def s3_handler(mock_aws_setup):
    """Fixture to create and return an instance of S3Handler."""
    return ProcessedS3Handler()


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({"column1": [1, 2], "column2": ["a", "b"]})


def test_get_new_file_name(s3_handler):
    """Test the get_new_file_name method."""
    table_name = "users"
    last_updated = "2025-02-26"
    expected_file_name = "users/2025-02-26.parquet.gzip"

    file_name = s3_handler.get_new_file_name(table_name, last_updated)
    assert file_name == expected_file_name


def test_upload_file_success(mock_aws_setup, s3_handler, sample_dataframe):
    """Test successful file upload to S3."""
    table_name = "users"
    timestamp = "20 12"

    response = s3_handler.upload_file(sample_dataframe, table_name, timestamp)

    assert "Success" in response
    assert (
        f"File {table_name}/20-12.parquet.gzip has been added to test-bucket"
        in response["Success"]
    )


def test_save_last_timestamp_success(mock_aws_setup, s3_handler):
    timestamp = "2025-02-26T12:00:00"
    response = s3_handler.save_last_timestamp(timestamp)

    assert "Success" in response
    assert (
        "File last_timestamp.txt has been updated in test-bucket"
        in response["Success"]
    )
    s3_client = boto3.client("s3", region_name="eu-west-2")
    obj = s3_client.get_object(Bucket="test-bucket", Key="last_timestamp.txt")
    body = obj["Body"].read().decode("utf-8")
    assert body == timestamp


def test_process_and_upload(mocker, s3_handler, sample_dataframe):
    mock_upload_file = mocker.patch.object(s3_handler, "upload_file")
    mock_upload_file.return_value = {"Success": "File uploaded"}
    mock_save_timestamp = mocker.patch.object(
        s3_handler, "save_last_timestamp"
    )

    sample_data = {"users": sample_dataframe, "orders": sample_dataframe}

    s3_handler.process_and_upload(sample_data, None)

    assert mock_upload_file.call_count == 2
    assert mock_save_timestamp.call_count == 1
