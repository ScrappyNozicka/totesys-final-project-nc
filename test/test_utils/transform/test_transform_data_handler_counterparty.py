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
    """Mock IngestionS3Handler to return fake counterparty data."""
    mock_handler = mocker.patch(
        "src.transform.transform_utils.transform_data_handler.IngestionS3Handler"
    )
    mock_instance = mock_handler.return_value
    mock_instance.get_data_from_ingestion.return_value = {
        "counterparty": [
            {
                "counterparty_id": 1,
                "counterparty_legal_name": "Fahey and Sons",
                "legal_address_id": 1,
                "commercial_contact": "Micheal Toy",
                "delivery_contact": "Mrs. Lucy Runolfsdottir",
                "created_at": "2022-11-03 14:20:51.563",
                "last_updated": "2022-11-03 14:20:51.563",
            },
            {
                "counterparty_id": 2,
                "counterparty_legal_name": "Leannon, Predovic and Morar",
                "legal_address_id": 2,
                "commercial_contact": "Melba Sanford",
                "delivery_contact": "Jean Hane III",
                "created_at": "2022-11-03 14:20:51.563",
                "last_updated": "2022-11-03 14:20:51.563",
            },
        ],
        "address": [
            {
                "address_id": 1,
                "address_line_1": "6826 Herzog Via",
                "address_line_2": "",
                "district": "Avon",
                "city": "New Patienceburgh",
                "postal_code": "28441",
                "country": "Turkey",
                "phone": "1803 637401",
                "created_at": "2022-11-03 14:20:49.962",
                "last_updated": "2022-11-03 14:20:49.962",
            },
            {
                "address_id": 2,
                "address_line_1": "179 Alexie Cliffs",
                "address_line_2": "",
                "district": "",
                "city": "Aliso Viejo",
                "postal_code": "99305-7380",
                "country": "San Marino",
                "phone": "9621 880720",
                "created_at": "2022-11-03 14:20:49.962",
                "last_updated": "2022-11-03 14:20:49.962",
            },
        ],
    }
    return mock_instance
# @pytest.fixture
def test_transform_counterparty_data(mock_ingestion_s3_handler):
    """Test counterparty data transformation."""
    test_variable = PandaTransformation()
    df_result = test_variable.transform_counterparty_data()
    expected_df = pd.DataFrame(
        {
            "counterparty_id": [1, 2],
            "counterparty_legal_name": [
                "Fahey and Sons",
                "Leannon, Predovic and Morar",
            ],
            "counterparty_legal_address_line_1": [
                "6826 Herzog Via",
                "179 Alexie Cliffs",
            ],
            "counterparty_legal_address_line_2": ["", ""],
            "counterparty_legal_district": ["Avon", ""],
            "counterparty_legal_city": ["New Patienceburgh", "Aliso Viejo"],
            "counterparty_legal_postal_code": ["28441", "99305-7380"],
            "counterparty_legal_country": ["Turkey", "San Marino"],
            "counterparty_legal_phone_number": ["1803 637401", "9621 880720"],
        }
    )
    pd.testing.assert_frame_equal(df_result, expected_df)