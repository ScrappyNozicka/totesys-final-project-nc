import boto3
import os
import json
from dotenv import load_dotenv
import botocore.exceptions
from warehouse_connection import create_conn


class DataWarehouseLoader:
    def __init__(self):
        load_dotenv()
        self.s3_client = boto3.client("s3")
        self.processing_bucket = os.getenv("PROCESSED_S3_BUCKET_NAME")
        self.timestamp_file_key = "last_inserted_timestamp.txt"
        self.conn = create_conn()
        
    def get_last_inserted_timestamp(self) -> str:
        """Retrieve the last inserted timestamp from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.processing_bucket, Key=self.timestamp_file_key)
            if "Body" in response:
                return response["Body"].read().decode("utf-8").strip()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                print("No previous timestamp found, assuming first run.")
                return "0000-00-00 00:00:00" 
        except Exception as e:
            print(f"Unexpected error fetching  timestamp: {e}")
        return None
    

    def update_last_inserted_timestamp(self, latest_timestamp: str):
        """Update the last inserted timestamp file in S3."""
        self.s3_client.put_object(
            Bucket=self.processing_bucket, Key=self.timestamp_file_key, Body=latest_timestamp.encode("utf-8")
        )    


    def get_new_files(self, last_timestamp: str):
        """Retrieve list of files in S3 that have a timestamp newer than the last inserted timestamp."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.processing_bucket)
            files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]
            return [f for f in files if f.split("/")[-1].replace(".json", "") > last_timestamp]
        except Exception as e:
            print(f"Error fetching file list: {e}")
            return []
        
    def insert_file_to_warehouse(self, file_key: str):
        """Load data from a JSON file in S3 into the data warehouse."""
        try:
            response = self.s3_client.get_object(Bucket=self.processing_bucket, Key=file_key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            table_name = file_key.split("/")[0]  

            placeholders = ", ".join(["%s"] * len(data[0]))
            columns = ", ".join(data[0].keys())  
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            for row in data:
                values = tuple(row.values())
                self.conn.run(query, values)
        except Exception as e:
            print(f"Error inserting data for {file_key}: {e}")
            return None   
        
    def process_new_files(self):
        """Process new files and update the last inserted timestamp."""
        last_timestamp = self.get_last_inserted_timestamp()
        new_files = self.get_new_files(last_timestamp)

        if not new_files:
            print("No new data to insert.")
            return

        
        for file_key in new_files:
            self.insert_file_to_warehouse(file_key)
  

        latest_insertion_timestamp = max([f.split("/")[-1].replace(".json", "") for f in new_files])
        self.update_last_inserted_timestamp(latest_insertion_timestamp)
        print(f"Updated last inserted timestamp to: {latest_insertion_timestamp}")
    