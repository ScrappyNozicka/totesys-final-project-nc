import json
from totesys_processor import ToteSysProcessor
from s3_file_handler import S3FileHandler

class DataIngestionHandler:
    """Main class to process ToteSys data and store it in S3."""

    def __init__(self):
        self.s3_handler = S3FileHandler()
        self.processor = ToteSysProcessor()

    def process_and_upload(self, totesys_data):
        table_names = self.processor.get_table_names(totesys_data)

        for table_name in table_names:
            table_data = totesys_data[table_name]

            for row in table_data:
                row_id = self.processor.get_row_id(row, table_name)
                last_updated = self.processor.get_last_updated(row)
                file_data = json.dumps(row)
                file_name = self.s3_handler.get_new_file_name(table_name, row_id, last_updated)

                self.s3_handler.upload_file(file_data, file_name)
