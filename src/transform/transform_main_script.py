from datetime import datetime
import logging
from transform_utils.transform_data_handler import PandaTransformation
from transform_utils.processed_s3_handler import ProcessedS3Handler

#Set up the logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def transform_main_script(event, context):
    
    try:
        processed_s3_handler = ProcessedS3Handler()
        panda_transform = PandaTransformation()
        current_timestamp = str(datetime.now())

        table_data_frames = panda_transform.returns_dictionary_of_dataframes()
        logging.info("Data successfully retrieved from Ingestion Bucket and transformed.")

        processed_s3_handler.process_and_upload(table_data_frames, current_timestamp)
        logging.info("Data processed and uploaded to Processed Bucket.")

        return {"message":"Updated successfully"}

    except Exception as e:
       logging.error(f"ERROR - Update failed:{e}")
       return {"message": "Update failed"}
    