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
    """Mock IngestionS3Handler to return fake staff data."""
    mock_handler = mocker.patch(
        "src.transform.transform_utils.transform_data_handler.IngestionS3Handler"
    )
    mock_instance = mock_handler.return_value
    mock_instance.get_data_from_ingestion.return_value = {
        "staff": [
            {
                "staff_id": 1,
                "first_name": "Jeremie",
                "last_name": "Franey",
                "department_id": 1,
                "email_address": "jeremie.franey@terrifictotes.com",
                "created_at": "2022-11-03 14:20:51.56",
                "last_updated": "2022-11-03 14:20:51.563",
            },
            {
                "staff_id": 2,
                "first_name": "Deron",
                "last_name": "Beier",
                "department_id": 2,
                "email_address": "deron.beier@terrifictotes.com",
                "created_at": "2022-11-03 14:20:51.56",
                "last_updated": "2022-11-03 14:20:51.563",
            },
        ],
        "department": [
            {
                "department_id": 1,
                "department_name": "Sales",
                "location": "Manchester",
                "manager": "Richard Roma",
                "created_at": "2022-11-03 14:20:49.962",
                "last_updated": "2022-11-03 14:20:49.962",
            },
            {
                "department_id": 2,
                "department_name": "Purchasing",
                "location": "Manchester",
                "manager": "Naomi Lapaglia",
                "created_at": "2022-11-03 14:20:49.962",
                "last_updated": "2022-11-03 14:20:49.962",
            },
        ],
    }
    return mock_instance


def test_transform_currency_data(mock_ingestion_s3_handler):
    """Test location data transformation."""
    test_variable = PandaTransformation()
    df_result = test_variable.transform_staff_data()
    expected_df = pd.DataFrame(
        {
            "staff_id": [1, 2],
            "first_name": ["Jeremie", "Deron"],
            "last_name": ["Franey", "Beier"],
            "department_name": ["Sales", "Purchasing"],
            "location": ["Manchester", "Manchester"],
            "email_address": [
                "jeremie.franey@terrifictotes.com",
                "deron.beier@terrifictotes.com",
            ],
        }
    )
    pd.testing.assert_frame_equal(df_result, expected_df)
