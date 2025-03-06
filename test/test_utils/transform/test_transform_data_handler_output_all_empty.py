import pytest
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
def mock_currency_lookup(mocker):
    """Mock JSON currency lookup file."""
    mocker.patch(
        "builtins.open",
        mocker.mock_open(
            read_data='{"USD": "United States Dollar", "EUR": "Euro"}'),
    )
    mocker.patch(
        "json.load", return_value={"USD": "United States Dollar", "EUR": "Euro"}
    )


@pytest.fixture
def mock_ingestion_s3_handler(mocker, mock_currency_lookup):
    """Mock IngestionS3Handler to return fake data."""
    mock_handler = mocker.patch(
        "src.transform.transform_utils.transform_data_handler.IngestionS3Handler"
    )
    mock_instance = mock_handler.return_value
    mock_instance.get_data_from_ingestion.return_value = None
    return mock_instance


def test_dataframes_dictionary_handles_empty_lists(mock_ingestion_s3_handler):
    """Test if function returns None if all dataframes are empty."""
    test_variable = PandaTransformation()
    # test_variable.returns_dictionary_of_dataframes()
    result = test_variable.returns_dictionary_of_dataframes()
    expected = None
    assert result == expected
