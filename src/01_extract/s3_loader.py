import boto3
from datetime import datetime
import json

s3_client = boto3.client("s3")
bucket_name = "IngestionBucketKettsLough"

# Reading S3 Data
def get_next_record_id(bucket_name: str) -> int:
    """
    Get the next record ID based on existing files in the S3 bucket.

    Parameters:
        bucket_name (str): Name of the S3 bucket.

    Returns:
        int: The next record ID (incremented from the highest found).
    """
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    
    # if no files in s3, return 1
    if "Contents" not in response:
        return 1

    # extracted record_ids
    record_ids = []

    for obj in response["Contents"]:
        # tables/record_id/timestamp
        name_parts = obj["Key"].split("/")
        
        if name_parts[1].isdigit():
            record_ids.append(int(name_parts[1]))

    return max(record_ids) + 1


def get_last_timestamp(bucket_name: str) -> str | None:
    """
    Retrieve the most recent timestamp from the filenames in the S3 bucket.

    Parameters:
        bucket_name (str): Name of the S3 bucket.

    Returns:
        str or None: The most recent timestamp in string format, or None if no files exist.
    """
    def is_valid_timestamp(timestamp_str):
        """Check if a string is a valid timestamp in the format 'YYYY-MM-DD HH:MM:SS'."""
        try:
            datetime.strptime(timestamp_str, "%Y-%m-%d_%H:%M:%S")  # Adjust format if needed
            return True
        except ValueError:
            return False
        
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    # Extracted record_ids
    timestamps = []
    # if no files in s3, return None
    if "Contents" not in response:
        return None
    
    for obj in response["Contents"]:
        name_parts = obj["Key"].split("/")
        if is_valid_timestamp(name_parts[2]):
            timestamps.append(name_parts[2])

    return max(timestamps)

# ToteSys DB Data
def get_last_updated(file_data: dict) -> str | None:
    """
    Retrieve the most recent 'last_updated' timestamp from the source database data.

    Parameters:
        file_data (list): A list of dictionaries, each representing a table, containing a 'last_updated' field.

    Returns:
        str or None: The most recent timestamp as a string, or None if no timestamps are found.
    """
    last_updated_timestamps = []
    for table in file_data:
        for timestamp in table["last_updated"]:
            last_updated_timestamps.append(timestamp)

    if last_updated_timestamps:
        return max(last_updated_timestamps)
    
    return None

# Loading S3 Data
def load_to_s3(file, file_name):
    """
    Upload a JSON file to the S3 bucket.

    Parameters:
        file (str): JSON string containing the data to be uploaded.
        file_name (str): The S3 key (file path) under which the file will be stored.

    Returns:
        dict or str: Success message if upload is successful, error message otherwise.
    """
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
    timestamp_for_file_name = last_updated_row_timestamp.replace(" ", "_")
    file_name = f"Tables/{record_id}/{timestamp_for_file_name}"

    if last_updated_row_timestamp > last_updated_file_timestamp:
        file = json.dumps(file_data,)
        load_to_s3(file, file_name)

    else:
        return "up to date"

