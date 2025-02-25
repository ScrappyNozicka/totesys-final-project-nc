import boto3
from datetime import datetime
import json

s3_client = boto3.client("s3")
bucket_name = "IngestionBucketKettsLough"



def get_next_record_id(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    
    if "Contents" not in response:
        return 1  #for first file

    # Extracted record_ids
    record_ids = []
    for obj in response["Contents"]:
        name_parts = obj["Key"].split("/")
        if name_parts[1].isdigit():
            record_ids.append(int(name_parts[1]))

    return max(record_ids) + 1 
# retrieve the timestamps from the titles of the files that are in the S3 bucket and returns the most recent timestamp
def get_last_timestamp():

    def is_valid_timestamp(timestamp_str):
        try:
            datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")  # Adjust format if needed
            return True
        except ValueError:
            return False
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    # Extracted record_ids
    timestamps = []

    if "Contents" not in response:
        return 1  #for first file
    
    for obj in response["Contents"]:
        name_parts = obj["Key"].split("/")
        if is_valid_timestamp(name_parts[2]):
            timestamps.append(name_parts[2])

    return max(timestamps)

# retrieve the timestamps from the source database lastupdated column from all tables and returns the most recent timestamp
def get_last_updated(file_data):
    last_updated_timestamps = []
    for table in file_data:
        for timestamp in table["last_updated"]:
            last_updated_timestamps.append(timestamp)

    return max(last_updated_timestamps)

# uploading the file to S3 bucket
def load_to_s3(file, file_name):
    try:
        s3_client.put_object(Body=file, Bucket=bucket_name, Key=file_name)
        return {"Success": f"File {file_name} has been added to {bucket_name}"}
    except Exception as e:
        return f"Error: {e}"


def laod_to_s3_handler(file_data):
    # next record_id
    record_id = get_next_record_id()
    # the most recent timestamp from the source database
    last_updated_row_timestamp = get_last_updated(file_data)
    # the most recent timestamp from the files on S3
    last_updated_file_timestamp = get_last_timestamp()
    file_name = f"Tables/{record_id}/{last_updated_row_timestamp}"

    if last_updated_row_timestamp > last_updated_file_timestamp:
        file = json.dumps(file_data,)
        load_to_s3(file, file_name)

    else:
        return "up to date"

