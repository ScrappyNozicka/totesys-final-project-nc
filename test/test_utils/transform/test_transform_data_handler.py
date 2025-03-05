import pytest
import json
import pandas as pd
from unittest.mock import MagicMock
from src.transform.transform_utils.transform_data_handler import PandaTransformation
from src.transform.transform_utils.ingestion_s3_handler import IngestionS3Handler

@pytest.fixture(autouse=True)
def mock_aws_credentials(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "test")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")

@pytest.fixture
def mock_ingestion_s3_handler(mocker):
    """Mock IngestionS3Handler to return fake currency data."""
    mock_handler = mocker.patch("src.transform.transform_utils.transform_data_handler.IngestionS3Handler")
    mock_instance = mock_handler.return_value
    mock_instance.get_data_from_ingestion.return_value = {
        "currency": [
            {"currency_code": "USD", "created_at": "2024-03-01", "last_updated": "2024-03-05"},
            {"currency_code": "EUR", "created_at": "2024-03-01", "last_updated": "2024-03-05"},
        ]
    }
    return mock_instance

@pytest.fixture
def mock_currency_lookup(mocker):
    """Mock JSON currency lookup file."""
    mocker.patch("builtins.open", mocker.mock_open(read_data='{"USD": "United States Dollar", "EUR": "Euro"}'))
    mocker.patch("json.load", return_value={"USD": "United States Dollar", "EUR": "Euro"})

def test_transform_currency_data(mock_ingestion_s3_handler, mock_currency_lookup):
    """Test currency data transformation."""
    test_variable = PandaTransformation()
    df_result = test_variable.transform_currency_data()
    expected_df = pd.DataFrame({
        "currency_code": ["USD", "EUR"],
        "currency_name": ["United States Dollar", "Euro"]
    })

    pd.testing.assert_frame_equal(df_result, expected_df)




#test input file valid format
#test input file invalid format


#test input file valid name
#test input file invalid name

#test return expected format
#test return value

#test output file valid name
#test output file invalid name
#test output file valid format
#test output file invalid format

#test currency retun expected value
#test currency return expected columns
#test currency return expected types
#test currency return expected length