import boto3
import os
import io
from dotenv import load_dotenv
import botocore.exceptions
import pandas as pd


class ProcessedS3Handler:
    """Handles interactions with S3 Processed, including uploading files."""

    def __init__(self):
        load_dotenv()
        self.bucket_name = os.getenv("PROCESSED_S3_BUCKET_NAME")
        self.s3_client = boto3.client("s3")

    def get_new_file_name(
        self, table_name: str, processing_timestamp: str
    ) -> str:
        """
        Generate filename for new table

        Args:
            table_name (str): Name of the table
            processing_timestamp (str): Timestamp of processing invocation

        Returns:
            str: Filename
        """
        timestamp = processing_timestamp.replace(" ", "-")
        return f"{table_name}/{timestamp}.parquet.gzip"

    def upload_file(
        self,
        data_frame: pd.DataFrame,
        table_name: str,
        processing_timestamp: str,
    ):
        """
        Load table as a new file into S3 bucket
        Args:
            data_frame : DataFrame to upload
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
            f = io.BytesIO()
            data_frame.to_parquet(f, compression="gzip")
            f.seek(0)

            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=file_name, Body=f
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
                print(f"ERROR: {e}")
        except Exception as e:
            # TODO: Replace with proper logging if needed
            print(f"Unexpected error fetching last timestamp: {e}")
        return None

    def process_and_upload(
        self, data: dict[pd.DataFrame], processing_timestamp: str
    ):
        """
        Handler of data from S3 Ingestion. Saves data into
        S3 bucket specified by environment variable.

        Args:
            data (dict[pd.DataFrame]): Data from S3 Ingestion
            processing_timestamp (str): Timestamp string of processing
        """

        for table_name, data_frame in data.items():

            if not data_frame.empty:
                self.upload_file(data_frame, table_name, processing_timestamp)

        self.save_last_timestamp(processing_timestamp)
