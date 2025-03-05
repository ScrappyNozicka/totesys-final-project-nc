import boto3
import os
import json
from dotenv import load_dotenv
import botocore.exceptions
import logging

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


class IngestionS3Handler:

    def __init__(self):
        load_dotenv()
        self.bucket_name = os.getenv("S3_BUCKET_NAME")
        self.s3_client = boto3.client("s3")

    def get_last_timestamp(self) -> str | None:
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key="last_timestamp.txt"
            )
            if "Body" in response:
                return response["Body"].read().decode("utf-8").strip()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logging.info(f"INFO: {e} - No existing timestamp")
        except Exception as e:
            logging.error(
                f"ERROR: Unexpected error fetching last timestamp: {e}"
            )
        return None

    def get_file_name(self, table_name: str, timestamp: str) -> str:
        """
        Generate filename for new row of data

        Args:
            table_name (str): Name of the table
            processing_timestamp (str): Timestamp of processing invocation

        Returns:
            str: Filename
        """
        timestamp_file = timestamp.replace(" ", "-")
        return f"{table_name}/{timestamp_file}.json"

    def get_table_content(self, file_name: str):
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=file_name
            )
            if "Body" in response:
                return response["Body"].read().decode("utf-8")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logging.info(f"INFO: {e} - No new data in table")
        except Exception as e:
            logging.error(
                f"ERROR: Unexpected error fetching last timestamp: {e}"
            )
        return None

    def get_data_from_ingestion(self):
        last_timestamp = self.get_last_timestamp()
        if last_timestamp:
            table_names = [
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
            result = {}

        for table_name in table_names:
            file_name = self.get_file_name(table_name, last_timestamp)
            file_data_json = self.get_table_content(file_name)

            if file_data_json is None:
                logging.info(f"No data found for {table_name}")
                continue
            try:
                file_data = json.loads(file_data_json)
                result[table_name] = file_data
            except json.JSONDecodeError as e:
                logging.error(
                    f"ERROR: decoding JSON for table {table_name}: {e}"
                )
            except Exception as e:
                logging.error(
                    f"ERROR: Unexpected error for table {table_name}: {e}"
                )

        return result
