import boto3
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def creates3ingestion(our_bucket_name):
    s3 = boto3.resource("s3")

    s3.create_bucket(
        Bucket=our_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    bucket = s3.Bucket(our_bucket_name)

    versioning = bucket.Versioning()

    versioning.enable()

    return bucket


creates3ingestion("ingestionbucketkettslough")
