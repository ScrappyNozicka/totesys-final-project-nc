import boto3
import os
from dotenv import load_dotenv

class S3FileHandler:
    """Handles interactions with AWS S3, including uploading files."""

    def __init__(self):
        load_dotenv()
        self.bucket_name = os.getenv("S3_BUCKET_NAME")
        self.s3_client = boto3.client("s3")

    def get_new_file_name(self, table_name, row_id, last_updated):
        return f"{table_name}/{row_id}/{last_updated}"

    def upload_file(self, file_data, file_name):
        try:
            self.s3_client.put_object(Body=file_data, Bucket=self.bucket_name, Key=file_name)
            return {"Success": f"File {file_name} has been added to {self.bucket_name}"}
        except Exception as e:
            return {"Error": str(e)}
