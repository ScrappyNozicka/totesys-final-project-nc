import pytest
import pandas as pd
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


@pytest.fixture
def mock_data():
    """Returns fake design data."""
    mock_data = {
        "design": [
            {
                "design_id": 8,
                "created_at": "2022-11-03 14:20:49.962",
                "design_name": "Wooden",
                "file_location": "/usr",
                "file_name": "wooden-20220717-npgz.json",
                "last_updated": "2023-01-12 18:50:09.935",
            },
            {
                "design_id": 51,
                "created_at": "2023-01-12 18:50:09.935",
                "design_name": "Bronze",
                "file_location": "/private",
                "file_name": "bronze-20221024-4dds.json",
                "last_updated": "2023-01-12 18:50:09.935",
            },
        ]
    }
    return mock_data


def test_transform_currency_data(mock_data, mocker):
    """Test location design transformation."""
    mocker.patch.object(
        IngestionS3Handler, "get_data_from_ingestion", return_value=mock_data
    )
    test_variable = PandaTransformation()
    df_result = test_variable.transform_design_data()
    expected_df = pd.DataFrame(
        {
            "design_id": [8, 51],
            "design_name": ["Wooden", "Bronze"],
            "file_location": ["/usr", "/private"],
            "file_name": [
                "wooden-20220717-npgz.json",
                "bronze-20221024-4dds.json",
            ],
        }
    )

    pd.testing.assert_frame_equal(df_result, expected_df)
