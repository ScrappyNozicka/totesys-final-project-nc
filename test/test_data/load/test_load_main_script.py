import pytest
import boto3
from moto import mock_aws  
from src.load.load_main_script import DataWarehouseLoader
from unittest.mock import patch, MagicMock
import pandas as pd
import io
from sqlalchemy import create_engine, text

@pytest.fixture
def s3_mock():
    """Set up a mocked S3 environment."""
    with mock_aws():
        region = "eu-west-2"
        s3 = boto3.client("s3", region_name=region)
        bucket_name = "test-processed-bucket"
        
        # Create the bucket with the region configuration
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
        
        # Upload a fake timestamp file
        s3.put_object(
            Bucket=bucket_name,
            Key="last_inserted_timestamp.txt",
            Body=b"2024-03-01 12:00:00:000000",
        )

        # Upload some fake .parquet files with timestamps in the filename
        s3.put_object(Bucket=bucket_name, Key="table/2024-03-01_00:00:00:000000.parquet.gzip", Body=b"data")
        s3.put_object(Bucket=bucket_name, Key="table/2024-03-02_00:00:00:000000.parquet.gzip", Body=b"data")
        s3.put_object(Bucket=bucket_name, Key="table/2024-03-03_00:00:00:000000.parquet.gzip", Body=b"data")

        yield bucket_name, s3  # Return bucket name and s3 client for testing

@pytest.fixture
def mock_db_engine():
    """Mock the SQLAlchemy engine and connection."""
    with patch('sqlalchemy.create_engine') as mock_engine:
        mock_conn = MagicMock()
        mock_engine.return_value.connect.return_value = mock_conn
        yield mock_engine, mock_conn

def test_get_last_inserted_timestamp(s3_mock, monkeypatch):
    """Test retrieving the last inserted timestamp."""
    bucket_name, _ = s3_mock

    # Mock environment variables
    monkeypatch.setenv("PROCESSED_S3_BUCKET_NAME", bucket_name)

    loader = DataWarehouseLoader()
    timestamp = loader.get_last_inserted_timestamp()

    assert timestamp == "2024-03-01 12:00:00:000000"

def test_update_last_inserted_timestamp(s3_mock, monkeypatch):
    """Test updating the last inserted timestamp."""
    bucket_name, s3 = s3_mock

    # Mock environment variables
    monkeypatch.setenv("PROCESSED_S3_BUCKET_NAME", bucket_name)

    # Create a DataWarehouseLoader instance
    loader = DataWarehouseLoader()

    # New timestamp to update
    new_timestamp = "2024-03-02 15:30:45:000000"

    # Call the method to update the timestamp
    loader.update_last_inserted_timestamp(new_timestamp)

    # Verify that the put_object method was called with the correct arguments
    response = s3.get_object(Bucket=bucket_name, Key="last_inserted_timestamp.txt")
    updated_timestamp = response["Body"].read().decode("utf-8")

    assert updated_timestamp == new_timestamp

def test_get_new_files(s3_mock, monkeypatch):
    """Test getting new files that have a timestamp later than the last inserted timestamp."""
    bucket_name, s3 = s3_mock

    # Mock environment variables
    monkeypatch.setenv("PROCESSED_S3_BUCKET_NAME", bucket_name)

    # Create a DataWarehouseLoader instance
    loader = DataWarehouseLoader()

    # Provide the last timestamp 
    last_timestamp = "2024-03-02_00:00:00:000000"

    # Call the method to get new files
    new_files = loader.get_new_files(last_timestamp)

    # Expected files with timestamps after "2024-03-02_00:00:00:000000"
    expected_files = [
        "table/2024-03-03_00:00:00:000000.parquet.gzip"
    ]

    # Verify the returned new files
    assert new_files == expected_files

@patch('pandas.DataFrame.to_sql')
def test_insert_file_to_warehouse(mock_to_sql, s3_mock, monkeypatch):
    """Test inserting data from a Parquet file into the data warehouse."""
    bucket_name, s3 = s3_mock

    # Mock environment variables
    monkeypatch.setenv("PROCESSED_S3_BUCKET_NAME", bucket_name)

    # Create a DataWarehouseLoader instance
    loader = DataWarehouseLoader()

    # Mock the Parquet file content
    parquet_data = pd.DataFrame({
        'column1': [1, 2, 3],
        'column2': ['a', 'b', 'c']
    })
    parquet_bytes = io.BytesIO()
    parquet_data.to_parquet(parquet_bytes, engine='pyarrow', compression='gzip')
    parquet_bytes.seek(0)

    # Upload the mock Parquet file to S3
    file_key = "table/2024-03-03_00:00:00:000000.parquet.gzip"
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=parquet_bytes.getvalue())

    # Call the method to insert the file into the warehouse
    loader.insert_file_to_warehouse(file_key)

    # Verify that to_sql was called
    mock_to_sql.assert_called_once()

@patch('pandas.DataFrame.to_sql')
def test_process_new_files(mock_to_sql, s3_mock, monkeypatch):
    """Test processing new files and updating the last inserted timestamp."""
    bucket_name, s3 = s3_mock

    # Mock environment variables
    monkeypatch.setenv("PROCESSED_S3_BUCKET_NAME", bucket_name)

    # Create a DataWarehouseLoader instance
    loader = DataWarehouseLoader()

    # Mock the Parquet file content
    parquet_data = pd.DataFrame({
        'column1': [1, 2, 3],
        'column2': ['a', 'b', 'c']
    })
    parquet_bytes = io.BytesIO()
    parquet_data.to_parquet(parquet_bytes, engine='pyarrow', compression='gzip')
    parquet_bytes.seek(0)

    # Upload the mock Parquet file to S3
    file_key = "table/2024-03-03_00:00:00:000000.parquet.gzip"
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=parquet_bytes.getvalue())

    # Call the method to process new files
    loader.process_new_files()

    # Verify that to_sql was called
    mock_to_sql.assert_called_once()

    # Verify that the last inserted timestamp was updated
    response = s3.get_object(Bucket=bucket_name, Key="last_inserted_timestamp.txt")
    updated_timestamp = response["Body"].read().decode("utf-8")
    assert updated_timestamp == "2024-03-03_00:00:00:000000"