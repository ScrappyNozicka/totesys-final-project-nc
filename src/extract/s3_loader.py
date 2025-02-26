import boto3
from datetime import timedelta, datetime, timezone
import json

s3_client = boto3.client("s3")
bucket_name = "IngestionBucketKettsLough"




def s3_loader(file_data):
    cut_off_timestamp = datetime.now() - timedelta(hours=1) #get_latest_timestamp()

    primary_keys = {
        "counterparty": "counterparty_id",
        "currency": "currency_id",
        "department": "department_id",
        "design": "design_id",
        "staff": "staff_id",
        "sales_order": "sales_order_id",
        "address": "address_id",
        "payment": "payment_id",
        "purchase_order": "purchase_order_id",
        "payment_type": "payment_type_id",
        "transaction": "transaction_id"
    }

    last_updated_rows = []

    for table, rows in file_data.items():
        primary_key = primary_keys.get(table)
        if not primary_key:
            continue  

        for row in rows:
            # get the last_updated timestamp
            timestamp_field = row.get("last_updated") 
            if not timestamp_field:
                timestamp_field = row.get("created_at") 
            if isinstance(timestamp_field, str):
                timestamp_field = datetime.strptime(timestamp_field, "%Y-%m-%d_%H-%M-%S")


            if timestamp_field and timestamp_field > cut_off_timestamp:
                row_id = row.get(primary_key)
                timestamp = timestamp_field.strftime('%Y-%m-%d_%H-%M-%S')

                file_name = f"{bucket_name}/{table}/{row_id}/{timestamp}"
                upload_to_s3(row, file_name)
                row["file_name"] = file_name

                last_updated_rows.append(row)

    return last_updated_rows


# validate timestamp format
def is_valid_timestamp(timestamp_str):
    try:
        datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")  # Adjust format if needed
        return True
    except ValueError:
        return False
    

# get most recent timestamp in the S3 bucket
def get_latest_timestamp():
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    timestamps = []

    if not response.get("Contents"):
        return None


    for obj in response["Contents"]:
        name_parts = obj["Key"].split("/")
        if len(name_parts) > 3 and is_valid_timestamp(name_parts[3]): 
            timestamps.append(datetime.strptime(name_parts[3], "%Y-%m-%d_%H-%M-%S"))

    return max(timestamps) if timestamps else None


# uploding the files to S3
def upload_to_s3(row_data, file_name):
    try:
        s3_client.put_object(Body=json.dumps(row_data), Bucket=bucket_name, Key=file_name)
        return {"Success": f"File {file_name} has been added to {bucket_name}"}
    except Exception as e:
        return {"Error": str(e)}

















        
        




# def laod_to_s3_handler(file_data):
#     # next record_id
#     record_id = get_next_record_id()
#     # the most recent timestamp from the source database
#     last_updated_row_timestamp = get_last_updated(file_data)
#     # the most recent timestamp from the files on S3
#     last_updated_file_timestamp = get_last_timestamp()
#     file_name = f"Tables/{record_id}/{last_updated_row_timestamp}"

#     if last_updated_row_timestamp > last_updated_file_timestamp:
#         file = json.dumps(file_data,)
#         load_to_s3(file, file_name)

#     else:
#         return "up to date"

