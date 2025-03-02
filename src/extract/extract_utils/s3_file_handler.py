import boto3
import os
from dotenv import load_dotenv
import botocore.exceptions


class S3FileHandler:
    """Handles interactions with AWS S3, including uploading files."""

    def __init__(self):
        load_dotenv()
        self.bucket_name = os.getenv("S3_BUCKET_NAME")
        self.s3_client = boto3.client("s3")

    def get_new_file_name(
        self, table_name: str, processing_timestamp: str
    ) -> str:
        """
        Generate filename for new row of data

        Args:
            table_name (str): Name of the table
            processing_timestamp (str): Timestamp of processing invocation

        Returns:
            str: Filename
        """
        timestamp = processing_timestamp.replace(" ", "-")
        return f"{table_name}/{timestamp}"

    def upload_file(
        self, file_data, table_name: str, processing_timestamp: str
    ):
        """
        Load table row as a new file into S3 bucket
        Args:
            file_data : JSON to upload
            table_name (str): Name of table
            processing_timestamp (str): Timestamp of processing invocation
        Returns:
            dict: Success message if upload is successful,
                error message otherwise.
        """
        try:
            file_name = self.get_new_file_name(
                table_name, processing_timestamp
            )
            self.s3_client.put_object(
                Body=file_data, Bucket=self.bucket_name, Key=file_name
            )
            return {
                "Success": f"File {file_name} has been added to "
                f"{self.bucket_name}"
            }
        except Exception as e:
            return {"Error": str(e)}

    def save_last_timestamp(self, timestamp: str):
        try:
            self.s3_client.put_object(
                Body=timestamp,
                Bucket=self.bucket_name,
                Key="last_timestamp.txt",
            )
            return {
                "Success": f"File last_timestamp.txt has been updated in "
                f"{self.bucket_name}"
            }
        except Exception as e:
            return {"Error": str(e)}

    def get_last_timestamp(self) -> str | None:
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key="last_timestamp.txt"
            )
            if "Body" in response:
                return response["Body"].read().decode("utf-8").strip()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            else:
                # TODO: Replace with proper logging if needed
                print(f"ClientError fetching last timestamp: {e}")
                raise
        except Exception as e:
            # TODO: Replace with proper logging if needed
            print(f"Unexpected error fetching last timestamp: {e}")
            raise
