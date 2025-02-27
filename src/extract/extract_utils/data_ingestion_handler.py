import json
from src.extract.extract_utils.totesys_processor import ToteSysProcessor
from src.extract.extract_utils.s3_file_handler import S3FileHandler

class DataIngestionHandler:
    """Main class to process ToteSys data and store it in S3."""

    def __init__(self):
        self.s3_handler = S3FileHandler()
        self.processor = ToteSysProcessor()

    def process_and_upload(self, totesys_data: dict[list[dict]]):
        """
        Handler of raw data from ToteSys DataBase. Saves data into S3 bucket specified by environment variable

        Args:
            totesys_data (dict[list[dict]]): Data from ToteSys DB
        """
        table_names = self.processor.get_table_names(totesys_data)

        for table_name in table_names:
            # get list of rows in current table
            table_data = totesys_data[table_name]

            for row in table_data:
                row_id = self.processor.get_row_id(row, table_name)
                last_updated = self.processor.get_last_updated(row)
                file_data = json.dumps(row)
                file_name = self.s3_handler.get_new_file_name(table_name, row_id, last_updated)

                self.s3_handler.upload_file(file_data, file_name)


# retrive bucket's last_updated

# Approaches for timestamps:
# 1. Return dictionary in format: {"table_name": max("last_updated")}
# 2. Return string of last_updated maximum value across all tables: max("last_updated")
# 3. Return dictionary containing last updates for each record in table: 