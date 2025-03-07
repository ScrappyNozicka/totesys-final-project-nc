import pytest
from src.transform.transform_utils.transform_data_handler import (
    PandaTransformation,
)
from src.transform.transform_utils.ingestion_s3_handler import (
    IngestionS3Handler,
)


@pytest.fixture(autouse=True)
def mock_aws_credentials(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "test")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
    monkeypatch.setenv("PROCESSED_S3_BUCKET_NAME", "test")


@pytest.fixture
def mock_currency_lookup(mocker):
    """Mock JSON currency lookup file."""
    mocker.patch(
        "builtins.open",
        mocker.mock_open(
            read_data='{"USD": "United States Dollar", "EUR": "Euro"}'
        ),
    )
    mocker.patch(
        "json.load",
        return_value={"USD": "United States Dollar", "EUR": "Euro"},
    )


def test_dataframes_dictionary_handles_empty_lists(mocker):
    """Test if function returns None if all dataframes are empty."""
    mocker.patch.object(
        IngestionS3Handler, "get_data_from_ingestion", return_value=None
    )
    test_variable = PandaTransformation()
    result = test_variable.returns_dictionary_of_dataframes()
    expected = None
    assert result == expected
