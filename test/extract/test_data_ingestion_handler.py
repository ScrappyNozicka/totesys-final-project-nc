import pytest
import json
from decimal import Decimal
from datetime import datetime
from src.extract.extract_utils.data_ingestion_handler import (
    DataIngestionHandler,
)


@pytest.fixture
def sample_data():
    return {
        "orders": [
            {"orders_id": "123", "last_updated": "2024-02-25"},
            {"orders_id": "124", "last_updated": "2024-02-26"},
        ],
        "sales": [
            {"sales_id": "123", "last_updated": "2024-02-25"},
            {"sales_id": "124", "last_updated": "2024-02-26"},
        ],
    }


@pytest.fixture
def handler():
    return DataIngestionHandler()


def test_upload_file_call_count(mocker, handler, sample_data):
    """Test if upload_file is called the correct number of times."""
    # Mock the S3Handler upload_file method
    mock_upload_file = mocker.patch(
        "extract_utils.s3_file_handler.S3FileHandler.upload_file"
    )
    mock_upload_file.return_value = {"Success": "File uploaded"}
    mock_save_timestamp = mocker.patch(
        "extract_utils.s3_file_handler.S3FileHandler.save_last_timestamp"
    )

    handler.process_and_upload(sample_data, None)

    # Check if upload_file was called twice
    assert mock_upload_file.call_count == 2
    assert mock_save_timestamp.call_count == 1


def test_upload_file_arguments(mocker, handler, sample_data):
    """Test if upload_file is called with the correct arguments."""
    # Mock the S3Handler upload_file method
    mock_upload_file = mocker.patch(
        "extract_utils.s3_file_handler.S3FileHandler.upload_file"
    )
    mock_upload_file.return_value = {"Success": "File uploaded"}
    mocker.patch(
        "extract_utils.s3_file_handler.S3FileHandler.save_last_timestamp"
    )

    handler.process_and_upload(sample_data, "2024-02-26")

    # Validate arguments passed to upload_file
    expected_calls = [
        mocker.call(
            json.dumps(sample_data["orders"]),
            "orders",
            "2024-02-26",
        ),
        mocker.call(
            json.dumps(sample_data["sales"]),
            "sales",
            "2024-02-26",
        ),
    ]

    mock_upload_file.assert_has_calls(expected_calls, any_order=True)


def test_normalize_data(handler):
    table_data = [
        {
            "amount": Decimal("123.45"),
            "created_at": datetime(2025, 3, 2, 12, 0, 0),
            "name": "Test User",
        }
    ]

    handler.normalize_data(table_data)

    assert isinstance(table_data[0]["amount"], float)
    assert table_data[0]["amount"] == 123.45

    assert isinstance(table_data[0]["created_at"], str)
    assert table_data[0]["created_at"] == "2025-03-02 12:00:00"
