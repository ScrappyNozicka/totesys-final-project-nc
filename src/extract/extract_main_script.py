from dotenv import load_dotenv

from extract_utils.data_ingestion_handler import DataIngestionHandler
from extract_utils.get_data_from_db import get_data_from_db

def extract_main_script():
    """
    Main script for Extraction Phase.
    It takes data from ToteSys Database and saves it to S3 Bucket.
    
    Requirements(.env):
        - DB connection credentials
        - AWS credentials
        - Bucket Name
    """
    load_dotenv()

    totesys_data = get_data_from_db()
    ingestion_handler = DataIngestionHandler()

    ingestion_handler.process_and_upload(totesys_data)


if __name__ == "__main__":
    extract_main_script()