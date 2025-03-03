import pytest
from moto import mock_aws
import boto3
import os
import json
import botocore.exceptions
from src.transform.transform_utils.ingestion_s3_handler import (
    IngestionS3Handler,
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
        os.environ["S3_BUCKET_NAME"] = bucket_name

        yield s3_client


@pytest.fixture
def s3_handler():
    """Fixture to create and return an instance of S3Handler."""
    return IngestionS3Handler()


def test_get_new_file_name(s3_handler):
    """Test the get_new_file_name method."""
    table_name = "users"
    last_updated = "2025-02-26"
    expected_file_name = "users/2025-02-26.json"

    file_name = s3_handler.get_file_name(table_name, last_updated)
    assert file_name == expected_file_name


def test_get_last_timestamp_no_file(mock_aws_setup, s3_handler):
    result = s3_handler.get_last_timestamp()
    assert result is None


def test_get_last_timestamp_success(mock_aws_setup, s3_handler):
    timestamp = "2025-02-26T12:00:00"
    s3_client = boto3.client("s3", region_name="eu-west-2")
    s3_client.put_object(
        Bucket="test-bucket", Key="last_timestamp.txt", Body=timestamp
    )

    result = s3_handler.get_last_timestamp()
    assert result == timestamp


def test_get_table_content_success(mock_aws_setup, s3_handler):
    value = "TEST"
    s3_client = boto3.client("s3", region_name="eu-west-2")
    s3_client.put_object(Bucket="test-bucket", Key="test.txt", Body=value)

    result = s3_handler.get_table_content("test.txt")
    assert result == value


def test_get_data_from_ingestion(s3_handler, mocker):
    mocker.patch.object(s3_handler, "get_last_timestamp", return_value="test")
    mocker.patch.object(s3_handler, "get_file_name", return_value="test")
    mocker.patch.object(
        s3_handler,
        "get_table_content",
        return_value=json.dumps(["test", "test"]),
    )
    expected_key_names = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]

    result = s3_handler.get_data_from_ingestion()

    for key in result.keys():
        assert key in expected_key_names
