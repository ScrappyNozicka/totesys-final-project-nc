from dotenv import load_dotenv
from datetime import datetime
from extract_utils.data_ingestion_handler import DataIngestionHandler
from extract_utils.get_data_from_db import get_data_from_db
from extract_utils.s3_file_handler import S3FileHandler
import boto3
import os
import json
import logging
#Set up the logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def get_secret():
  """Fetch secrets from AWS Secrets Manager"""
  try:
      secret_name = os.environ["SECRET_NAME"]
      client = boto3.client("secretsmanager")
    
      response = client.get_secret_value(SecretId=secret_name)
      secret = json.loads(response["SecretString"])
      return secret
  except Exception as e:
       logging.error(f"Unable to obtain secrets: {e}")
       raise


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
       secrets = get_secret()
       os.environ["DB_USER"] = secrets["DB_USER"]
       os.environ["DB_PASSWORD"] = secrets["DB_PASSWORD"]
       os.environ["DB_HOST"] = secrets["DB_HOST"]
       os.environ["DB_NAME"] = secrets["DB_NAME"]
       os.environ["DB_PORT"] = str(secrets["DB_PORT"])
       s3_file_handler = S3FileHandler()
       ingestion_handler = DataIngestionHandler()
       current_timestamp = str(datetime.now())


       last_timestamp = s3_file_handler.get_last_timestamp()
       logging.info(f"Last timestamp retrieved: {last_timestamp}")
       totesys_data = get_data_from_db(last_timestamp, current_timestamp)
       logging.info("Data successfully retrieved from ToteSys database.")


       ingestion_handler.process_and_upload(totesys_data, current_timestamp)
       logging.info("Data processed and uploaded to S3.")


       return {"message":"Updated successfully"}
   except Exception as e:
       logging.error(f"ERROR - Update failed:{e}")
       return {"message": "Update failed"}