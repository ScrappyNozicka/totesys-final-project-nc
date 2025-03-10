import pytest
import botocore.exceptions
import pandas as pd
import io
from unittest.mock import MagicMock
from src.load.data_warehouse_loader import DataWarehouseLoader


@pytest.fixture
def mock_loader(mocker):
    """DataWarehouseLoader with mocked dependencies"""
    mocker.patch(
        "src.load.data_warehouse_loader.create_conn", return_value=MagicMock()
    )
    mocker.patch("boto3.client")  # Mock AWS credentials and interactions
    loader = DataWarehouseLoader()
    loader.s3_client = MagicMock()
    return loader


def test_get_last_inserted_timestamp_exists(mock_loader):
    mock_loader.s3_client.get_object.return_value = {
        "Body": io.BytesIO(b"2025-03-10 12:34:56:78900")
    }

    result = mock_loader.get_last_inserted_timestamp()
    assert (
        result == "2025-03-10 12:34:56:78900"
    ), "Should return correct timestamp"


def test_get_last_inserted_timestamp_no_file(mock_loader):
    mock_loader.s3_client.get_object.side_effect = (
        botocore.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )
    )

    result = mock_loader.get_last_inserted_timestamp()
    assert (
        result == "0000-00-00 00:00:00:00000"
    ), "Should return default timestamp if file does not exist"


def test_get_last_inserted_timestamp_unexpected_error(mock_loader):
    mock_loader.s3_client.get_object.side_effect = Exception("Unknown error")

    result = mock_loader.get_last_inserted_timestamp()
    assert result is None, "Should return None if an unexpected error occurs"


def test_update_last_inserted_timestamp(mock_loader):
    mock_loader.update_last_inserted_timestamp("2025-03-10 12:34:56:78900")

    mock_loader.s3_client.put_object.assert_called_once_with(
        Bucket=mock_loader.processing_bucket,
        Key="last_inserted_timestamp.txt",
        Body=b"2025-03-10 12:34:56:78900",
    )


def test_get_new_files(mock_loader):
    mock_loader.s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "dim_date/2025-03-10.parquet.gzip"},
            {"Key": "dim_date/2025-03-09.parquet.gzip"},
        ]
    }

    result = mock_loader.get_new_files("2025-03-09")
    assert result == [
        "dim_date/2025-03-10.parquet.gzip"
    ], "Should return only newer files"


def test_get_new_files_no_files(mock_loader):
    mock_loader.s3_client.list_objects_v2.return_value = {"Contents": []}

    result = mock_loader.get_new_files("2025-03-09")
    assert (
        result == []
    ), "Should return empty list if no Parquet files are found"


def test_insert_file_to_warehouse(mock_loader, mocker):
    df = pd.DataFrame({"id": [1, 2, 3]})
    f = io.BytesIO()
    df.to_parquet(f, index=False)
    f.seek(0)

    mock_loader.s3_client.get_object.return_value = {"Body": f}
    mock_to_sql = mocker.patch("pandas.DataFrame.to_sql")

    mock_loader.insert_file_to_warehouse("dim_date/2025-03-10.parquet.gzip")

    mock_to_sql.assert_called_once()


def test_process_new_files(mock_loader, mocker):
    mock_loader.get_last_inserted_timestamp = MagicMock(
        return_value="2025-03-09"
    )
    mock_loader.get_new_files = MagicMock(
        return_value=["dim_date/2025-03-10.parquet.gzip"]
    )
    mock_loader.insert_file_to_warehouse = MagicMock()
    mock_loader.update_last_inserted_timestamp = MagicMock()

    mock_loader.process_new_files()

    mock_loader.insert_file_to_warehouse.assert_called_once_with(
        "dim_date/2025-03-10.parquet.gzip"
    )
    mock_loader.update_last_inserted_timestamp.assert_called_once_with(
        "2025-03-10"
    )
