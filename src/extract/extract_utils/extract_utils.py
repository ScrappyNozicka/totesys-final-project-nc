from datetime import datetime
import boto3

def get_last_timestamp():
    s3_client = boto3.client("s3")
    bucket_name = "IngestionBucketKettsLough"

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
        return None  #for first file 
    
    for obj in response["Contents"]:
        name_parts = obj["Key"].split("/")
        if is_valid_timestamp(name_parts[2]):
            timestamps.append(name_parts[2])

    return max(timestamps)