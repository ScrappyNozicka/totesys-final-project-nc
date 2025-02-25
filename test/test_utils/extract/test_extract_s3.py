from src.extract.extract.extract_s3 import creates3ingestion
import boto3
import os
import pytest
from moto import mock_aws

@mock_aws
def test_create_bucket_creates_a_bucket():
    creates3ingestion()
    s3=boto3.client("s3")
    buckets = s3.list_buckets()

    assert len(buckets["Buckets"])==1
 
@mock_aws
def test_create_bucket_creates_bucket_with_correct_name():

    name_of_bucket = "IngestionBucketKettsLough"
    creates3ingestion()
    s3=boto3.client("s3")
    buckets = s3.list_buckets()
    assert buckets["Buckets"][0]["Name"] == name_of_bucket

@mock_aws
def test_create_bucket_creates_bucket_with_versioning_enabled():
    creates3ingestion()
    s3=boto3.client("s3")
    buckets = s3.list_buckets()
    response = s3.get_bucket_versioning(Bucket="IngestionBucketKettsLough")
    assert response["Status"] == "Enabled"