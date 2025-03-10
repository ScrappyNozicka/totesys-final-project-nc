import pytest
from unittest.mock import MagicMock
import botocore.exceptions
from src.transform.transform_utils.transform_data_handler import (
    PandaTransformation,
)


@pytest.fixture
def mock_transformation(mocker):
    module_path = "src.transform.transform_utils.transform_data_handler"

    mocker.patch(f"{module_path}.load_dotenv")
    mocker.patch(f"{module_path}.IngestionS3Handler")
    mocker.patch("boto3.client")

    instance = PandaTransformation()
    instance.s3_client = MagicMock()
    instance.processed_bucket_name = "test-processed-bucket"
    instance.dim_date_prefix = "dim_date/"
    return instance


def test_check_date_file_exists_true(mock_transformation):
    mock_transformation.s3_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "dim_date/file.txt"}]
    }

    result = mock_transformation.check_date_file_exists()

    assert result is True
    mock_transformation.s3_client.list_objects_v2.assert_called_once_with(
        Bucket="test-processed-bucket", Prefix="dim_date/"
    )


def test_check_date_file_exists_no_such_key(mock_transformation):
    error_response = {
        "Error": {"Code": "NoSuchKey", "Message": "File not found"}
    }
    mock_transformation.s3_client.list_objects_v2.side_effect = (
        botocore.exceptions.ClientError(error_response, "ListObjectsV2")
    )

    result = mock_transformation.check_date_file_exists()

    assert result is False


def test_check_date_file_exists_other_error(mock_transformation):
    error_response = {
        "Error": {"Code": "AccessDenied", "Message": "Permission Denied"}
    }
    mock_transformation.s3_client.list_objects_v2.side_effect = (
        botocore.exceptions.ClientError(error_response, "ListObjectsV2")
    )

    result = mock_transformation.check_date_file_exists()

    assert result is False
