from src.extract.extract_utils.extract_s3 import creates3ingestion
import boto3
import os
import pytest
from moto import mock_aws

@mock_aws
def test_create_bucket_creates_a_bucket():
    creates3ingestion("FakeTestBucket3000")
    s3=boto3.client("s3")
    buckets = s3.list_buckets()

    assert len(buckets["Buckets"])==1
 
@mock_aws
def test_create_bucket_creates_bucket_with_correct_name():

    name_of_bucket = "FakeTestBucket3000"
    creates3ingestion(name_of_bucket)
    s3=boto3.client("s3")
    buckets = s3.list_buckets()
    assert buckets["Buckets"][0]["Name"] == name_of_bucket

@mock_aws
def test_create_bucket_creates_bucket_with_versioning_enabled():
    name_of_bucket = "FakeTestBucket3000"
    creates3ingestion(name_of_bucket)
    s3=boto3.client("s3")
    buckets = s3.list_buckets()
    response = s3.get_bucket_versioning(Bucket="FakeTestBucket3000")
    assert response["Status"] == "Enabled"

#testing testing