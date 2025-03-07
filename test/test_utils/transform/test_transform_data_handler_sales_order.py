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
    """Returns fake sales order data."""
    mock_data = {
        "sales_order": [
            {
                "sales_order_id": 2,
                "created_at": "2022-11-03 14:20:52.186",
                "last_updated": "2022-11-03 14:20:52.186",
                "design_id": 3,
                "staff_id": 19,
                "counterparty_id": 8,
                "units_sold": 42972,
                "unit_price": 3.94,
                "currency_id": 2,
                "agreed_delivery_date": "2022-11-07",
                "agreed_payment_date": "2022-11-08",
                "agreed_delivery_location_id": 8,
            },
            {
                "sales_order_id": 3,
                "created_at": "2022-11-03 14:20:52.188",
                "last_updated": "2022-11-03 14:20:52.188",
                "design_id": 4,
                "staff_id": 10,
                "counterparty_id": 4,
                "units_sold": 65839,
                "unit_price": 2.91,
                "currency_id": 3,
                "agreed_delivery_date": "2022-11-06",
                "agreed_payment_date": "2022-11-07",
                "agreed_delivery_location_id": 19,
            },
        ]
    }
    return mock_data


def test_transform_sales_order_data(mock_data, mocker):
    """Test currency data transformation."""
    mocker.patch.object(
        IngestionS3Handler, "get_data_from_ingestion", return_value=mock_data
    )
    test_variable = PandaTransformation()
    df_result = test_variable.transform_sales_order_data()
    expected_df = pd.DataFrame(
        {
            "sales_order_id": [2, 3],
            "created_date": ["2022-11-03", "2022-11-03"],
            "created_time": ["14:20:52.186", "14:20:52.188"],
            "last_updated_date": ["2022-11-03", "2022-11-03"],
            "last_updated_time": ["14:20:52.186", "14:20:52.188"],
            "sales_staff_id": [19, 10],
            "counterparty_id": [8, 4],
            "units_sold": [42972, 65839],
            "unit_price": [3.94, 2.91],
            "currency_id": [2, 3],
            "design_id": [3, 4],
            "agreed_payment_date": ["2022-11-08", "2022-11-07"],
            "agreed_delivery_date": ["2022-11-07", "2022-11-06"],
            "agreed_delivery_location_id": [8, 19],
        }
    )
    pd.testing.assert_frame_equal(df_result, expected_df)
