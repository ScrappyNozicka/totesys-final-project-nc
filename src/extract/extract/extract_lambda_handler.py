
from src.extract.vars import BUCKET_NAME
from src.extract.extract.extract_s3 import creates3ingestion
# from src.extract.connection import create_conn
import boto3
import json
import pg8000
import os
from dotenv import load_dotenv
from datetime import datetime
import logging

s3 = boto3.client('s3')


ourbucket = creates3ingestion(BUCKET_NAME)

load_dotenv()

db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']
db_host = os.environ['DB_HOST']
db_name = os.environ['DB_NAME']
db_port = os.environ['DB_PORT']

def lambda_handler(event, context):
    
    try:
        connection = pg8000.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            database=db_name
        )
        
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM toyses")
            rows = cursor.fetchall()


        data = json.dumps(rows)

        time = event.get('time', str(datetime.now()))

        s3_key = f"data/{time}.json"

        s3.put_object(Bucket=ourbucket, Key=s3_key, Body=data)

        print(f"Successfully uploaded to S3://{ourbucket}/{s3_key}")

        return {
            'statusCode': 200,
            'body': json.dumps(f"Data successfully uploaded to {s3_key}")
        }

  
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise e
    