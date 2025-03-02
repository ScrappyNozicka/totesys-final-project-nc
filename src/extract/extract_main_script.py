from dotenv import load_dotenv
from datetime import datetime

from extract_utils.data_ingestion_handler import DataIngestionHandler
from extract_utils.get_data_from_db import get_data_from_db
from extract_utils.s3_file_handler import S3FileHandler

def extract_main_script(event, context):
    """
    Main script for Extraction Phase.
    It takes data from ToteSys Database and saves it to S3 Bucket.
    
    Requirements(.env):
        - DB connection credentials
        - AWS credentials
        - Bucket Name
    """
    try:
        load_dotenv()

        s3_file_handler = S3FileHandler()
        ingestion_handler = DataIngestionHandler()
        current_timestamp = str(datetime.now())

        last_timestamp = s3_file_handler.get_last_timestamp()

        totesys_data = get_data_from_db(last_timestamp, current_timestamp)

        ingestion_handler.process_and_upload(totesys_data, current_timestamp)

        return "Updated successfully"
    except:
        return "Update failed"
    