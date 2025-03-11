import boto3
import os
import json
from dotenv import load_dotenv
import botocore.exceptions
import logging
import pandas as pd

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
                logging.info("Last timestamp has been found.")
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
                logging.info(f"Data in table retrieved successfully")
                return response["Body"].read().decode("utf-8")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logging.info(f"INFO: {e} - No new data in table")
        except Exception as e:
            logging.error(f"ERROR: Unexpected error fetching last table: {e}")
        return None

    def get_full_table(self, table_name):
        try:
            table = table_name.split("_")[0]
            files = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=table
            )
            rows = []
            for obj in files["Contents"]:
                key = obj["Key"]

                response = self.s3_client.get_object(
                    Bucket=self.bucket_name, Key=key
                )
                if "Body" in response:
                    file_data_json = response["Body"].read().decode("utf-8")
                    file_data = json.loads(file_data_json)
                    rows.extend(file_data)

            df = pd.DataFrame(rows)
            df = df.sort_values(by="last_updated", ascending=False)
            df = df.drop_duplicates(subset=f"{table}_id", keep="first")

            return_list = df.to_dict("records")
            logging.info(f"Full table information retrieved for: {table}.")
            return return_list

        except botocore.exceptions.ClientError as e:
            logging.info(f"INFO: {e}")
        except Exception as e:
            logging.error(f"ERROR: Unexpected error fetching last table: {e}")
        return None

    def get_data_from_ingestion(self):
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
            "department_all_data",
            "address_all_data",
        ]
        result = {}
        last_timestamp = self.get_last_timestamp()

        if last_timestamp:

            for table_name in table_names:
                if table_name in ["department_all_data", "address_all_data"]:
                    file_data = self.get_full_table(table_name)
                    result[table_name] = file_data
                else:
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
                        logging.error(f"ERROR: {e} for table - {table_name}")

        return result
