import json
from decimal import Decimal
from datetime import datetime
from extract_utils.s3_file_handler import S3FileHandler


class DataIngestionHandler:
    """Main class to process ToteSys data and store it in S3."""

    def __init__(self):
        self.s3_handler = S3FileHandler()

    def normalize_data(self, table_data: list[dict]):
        """
        Normalizes the data by converting `Decimal` and `datetime` values to
        their appropriate types.

        Args:
            table_data (list[dict]): Table data
        """
        for row in table_data:
            for key, value in row.items():
                if isinstance(value, Decimal):
                    row[key] = float(value)
                if isinstance(value, datetime):
                    row[key] = str(value)

    def process_and_upload(
        self, totesys_data: dict[list[dict]], processing_timestamp: str
    ):
        """
        Handler of raw data from ToteSys DataBase. Saves data into
        S3 bucket specified by environment variable.

        Args:
            totesys_data (dict[list[dict]]): Data from ToteSys DB
            processing_timestamp (str): Timestamp string of processing
        """

        for table_name, table_data in totesys_data.items():

            if table_data:
                self.normalize_data(table_data)

                file_data = json.dumps(table_data)

                self.s3_handler.upload_file(
                    file_data, table_name, processing_timestamp
                )

        self.s3_handler.save_last_timestamp(processing_timestamp)
