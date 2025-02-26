import pytest
from moto import mock_aws
import boto3
import os
from s3_file_handler import S3FileHandler  # Replace with the actual path to your module

@pytest.fixture
def mock_aws_setup():
    """Setup a mock AWS S3 bucket using moto's mock_aws in the eu-west-2 region."""
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )

        # Set environment variable for bucket name (normally loaded from .env)
        os.environ["S3_BUCKET_NAME"] = bucket_name

        yield s3_client

@pytest.fixture
def s3_handler():
    """Fixture to create and return an instance of S3Handler."""
    return S3FileHandler()

def test_get_new_file_name(s3_handler):
    """Test the get_new_file_name method."""
    table_name = "users"
    row_id = "123"
    last_updated = "2025-02-26"
    expected_file_name = "users/123/2025-02-26"

    file_name = s3_handler.get_new_file_name(table_name, row_id, last_updated)
    assert file_name == expected_file_name

def test_upload_file_success(mock_aws_setup, s3_handler):
    """Test successful file upload to S3."""
    file_data = b"Test file content"
    file_name = "users/123/2025-02-26"

    response = s3_handler.upload_file(file_data, file_name)

    assert "Success" in response
    assert f"File {file_name} has been added to test-bucket" in response["Success"]

def test_upload_file_failure_due_to_permissions(s3_handler):
    """Test file upload failure due to permission issues."""
    # Simulate missing S3_BUCKET_NAME environment variable
    os.environ["S3_BUCKET_NAME"] = "non-existent-bucket"  # Mocking a non-existent bucket

    file_data = b"Test file content"
    file_name = "users/123/2025-02-26"

    # This should simulate an AccessDenied error because the bucket is missing or wrong
    response = s3_handler.upload_file(file_data, file_name)

    assert "Error" in response
    assert "Access Denied" in response["Error"]  # Checking for AccessDenied error


