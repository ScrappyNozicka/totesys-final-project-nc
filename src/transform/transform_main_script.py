from datetime import datetime
from transform_utils.processed_s3_handler import ProcessedS3Handler
from transform_utils.ingestion_s3_handler import IngestionS3Handler
import logging
#Set up the logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def transform_main_script(event, context):
    
    try:
        ingestion_s3_handler = IngestionS3Handler()
        processed_s3_handler = ProcessedS3Handler()
        current_timestamp = str(datetime.now())

        tables_ingestion_data = ingestion_s3_handler.get_data_from_ingestion()
        table_data_frames = format_ingestion_data(tables_ingestion_data)
        logging.info("Data successfully retrieved from Injestion Bucket.")


        processed_s3_handler.process_and_upload(table_data_frames, current_timestamp)
        logging.info("Data processed and uploaded to Processed Bucket.")

        return {"message":"Updated successfully"}

    except Exception as e:
       logging.error(f"ERROR - Update failed:{e}")
       return {"message": "Update failed"}