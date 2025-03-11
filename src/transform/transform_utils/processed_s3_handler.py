import boto3
import os
import io
from dotenv import load_dotenv
import pandas as pd
import logging

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


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
            logging.info(f"trying to upload file: {file_name}")
            f = io.BytesIO()
            data_frame.to_parquet(f, compression="gzip")
            f.seek(0)

            logging.info(f"Size of DataFrame: {data_frame.shape}")

            logging.info("compression to gzip success, starting put_object")
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=file_name, Body=f
            )

            logging.info(
                f"INFO: File {file_name} has been added to {self.bucket_name}"
            )
            return {
                "Success": f"File {file_name} has been added to "
                f"{self.bucket_name}"
            }
        except Exception as e:
            logging.error(
                f"ERROR: Unexpected error uploading file to s3 processed: {e}"
            )
            return {"Error": str(e)}

    def save_last_timestamp(self, timestamp: str):
        try:
            self.s3_client.put_object(
                Body=timestamp,
                Bucket=self.bucket_name,
                Key="last_timestamp.txt",
            )
            logging.info(
                f"File with last timestamp updated in {self.bucket_name}"
            )
            return {
                "Success": f"File last_timestamp.txt has been updated in "
                f"{self.bucket_name}"
            }
        except Exception as e:
            logging.error(
                f"ERROR: Unexpected error saving last timestamp: {e}"
            )
            return {"Error": str(e)}

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
        logging.info("Inside process and upload")
        for table_name, data_frame in data.items():
            if not data_frame.empty:
                self.upload_file(data_frame, table_name, processing_timestamp)
                logging.info(f"FILE UPLOADED FOR TABLE: {table_name}")

        self.save_last_timestamp(processing_timestamp)
