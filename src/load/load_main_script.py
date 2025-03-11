import logging
import boto3
import json
import os
from data_warehouse_loader import DataWarehouseLoader
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
       logging.error(f"ERROR: Unable to obtain secrets: {e}")
       raise

def load_main_script(event, context):
    """
    Main script for Load Phase.
    It takes data from S3 Bucket and saves it to final database called Postgres.
  
    Requirements(.env):
       - DB connection credentials
       - AWS credentials
       - Bucket Name
    """
    try:
        secrets = get_secret()
        os.environ["DW_USER"] = secrets["DW_USER"]
        os.environ["DW_PASSWORD"] = secrets["DW_PASSWORD"]
        os.environ["DW_HOST"] = secrets["DW_HOST"]
        os.environ["DW_NAME"] = secrets["DW_NAME"]
        os.environ["DW_PORT"] = str(secrets["DW_PORT"])
        
        logging.info("Uploading to DataWarehouse started")
        loader = DataWarehouseLoader()
        loader.process_new_files()
        logging.info("Data uploaded to DataWarehouse successfully")

        return {"message":"Updated successfully"}

    except Exception as e:
       logging.error(f"ERROR: Update failed:{e}")
       return {"message": "Update failed"}