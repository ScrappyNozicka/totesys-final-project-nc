import json
from extract_utils.totesys_processor import ToteSysProcessor
from extract_utils.s3_file_handler import S3FileHandler


class DataIngestionHandler:
    """Main class to process ToteSys data and store it in S3."""

    def __init__(self):
        self.s3_handler = S3FileHandler()
        self.processor = ToteSysProcessor()

    def process_and_upload(self, totesys_data: dict[list[dict]]):
        """
        Handler of raw data from ToteSys DataBase. Saves data into
        S3 bucket specified by environment variable.

        Args:
            totesys_data (dict[list[dict]]): Data from ToteSys DB
        """
        table_names = self.processor.get_table_names(totesys_data)

        for table_name in table_names:
            # get list of rows in current table
            table_data = totesys_data[table_name]
            rows = []
            last_updated_max = ""
            for row in table_data:
                last_updated = self.processor.get_last_updated(row)
                if last_updated > last_updated_max:
                    last_updated_max = last_updated
                row_json = json.dumps(
                    row, default=self.processor.decimal_to_str
                )
                rows.append(row_json)
            
            file_data = json.dumps(rows)

            file_name = self.s3_handler.get_new_file_name(table_name, last_updated_max)

            self.s3_handler.upload_file(file_data, file_name)
            print(table_name, "<--- UPLOADED SUCCESSFULLY")




            # for row in table_data:
            #     row_id = self.processor.get_row_id(row, table_name)
            #     last_updated = self.processor.get_last_updated(row)
            #     file_data = json.dumps(
            #         row, default=self.processor.decimal_to_str
            #     )
            #     file_name = self.s3_handler.get_new_file_name(
            #         table_name, row_id, last_updated
            #     )

            #     self.s3_handler.upload_file(file_data, file_name)
