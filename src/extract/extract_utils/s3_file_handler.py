import boto3
import os
from dotenv import load_dotenv


class S3FileHandler:
    """Handles interactions with AWS S3, including uploading files."""

    def __init__(self):
        load_dotenv()
        self.bucket_name = os.getenv("S3_BUCKET_NAME")
        self.s3_client = boto3.client("s3")

    def get_new_file_name(self, table_name, row_id, last_updated):
        """
        Generate filename for new row of data

        Args:
            table_name (_type_): table_name
            row_id (_type_): row_id value
            last_updated (_type_): last_updated value

        Returns:
            str: Filename
        """
        return f"{table_name}/{row_id}/{last_updated}"

    def upload_file(self, file_data, file_name):
        """
        Load table row as a new file into S3 bucket
        Args:
            file_data (_type_): JSON
            file_name (_type_): Name of new file
        Returns:
            dict or str: Success message if upload is successful,
                         error message otherwise.
        """
        try:
            self.s3_client.put_object(
                Body=file_data, Bucket=self.bucket_name, Key=file_name
            )
            return {
                "Success": f"File {file_name} has been added to "
                f"{self.bucket_name}"
            }
        except Exception as e:
            return {"Error": str(e)}

    def s3_timestamp_extraction(self):
        try:
            s3_paginator = self.s3_client.get_paginator('list_objects_v2')
            s3_iterator = s3_paginator.paginate(Bucket=self.bucket_name)
            lt = None
            for page in s3_iterator:
                if "Contents" in page:
                    lt2 = max(
                        page['Contents'],
                        key=lambda x: x['LastModified']
                        )
                    if lt is None or lt2['LastModified'] > lt['LastModified']:
                        lt = lt2
            timestamp = lt['LastModified']
            return timestamp.strftime("%Y, %m, %d, %H, %M, %S, %f")
        except Exception:
            return {"Error": "Unable to provide timestamp"}
