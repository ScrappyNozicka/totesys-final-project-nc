import pytest
import pandas as pd
from src.transform.transform_utils.transform_data_handler import PandaTransformation


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
    mock_handler = mocker.patch(
        "src.transform.transform_utils.transform_data_handler.IngestionS3Handler"
    )
    mock_instance = mock_handler.return_value
    mock_instance.get_data_from_ingestion.return_value = {
        "currency": [
            {
                "currency_code": "USD",
                "created_at": "2024-03-01",
                "last_updated": "2024-03-05",
            },
            {
                "currency_code": "EUR",
                "created_at": "2024-03-01",
                "last_updated": "2024-03-05",
            },
        ]
    }
    return mock_instance


@pytest.fixture
def mock_currency_lookup(mocker):
    """Mock JSON currency lookup file."""
    mocker.patch(
        "builtins.open",
        mocker.mock_open(read_data='{"USD": "United States Dollar", "EUR": "Euro"}'),
    )
    mocker.patch(
        "json.load", return_value={"USD": "United States Dollar", "EUR": "Euro"}
    )


def test_trans_currency_data(mock_ingestion_s3_handler, mock_currency_lookup):
    """Test currency data transformation."""
    test_var = PandaTransformation()
    df_result = test_var.transform_currency_data()
    expected_df = pd.DataFrame(
        {
            "currency_code": ["USD", "EUR"],
            "currency_name": ["United States Dollar", "Euro"],
        }
    )
    pd.testing.assert_frame_equal(df_result, expected_df)
