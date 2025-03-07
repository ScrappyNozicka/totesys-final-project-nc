import pytest
import pandas as pd
from src.transform.transform_utils.transform_data_handler import (
    PandaTransformation,
)
from src.transform.transform_utils.ingestion_s3_handler import (
    IngestionS3Handler,
)


@pytest.fixture
def mock_sales_order_data():
    return {
        "sales_order": [
            {
                "created_at": "2024-01-01 10:00:00",
                "last_updated": "2024-01-02 11:00:00",
                "agreed_payment_date": "2024-01-03 12:00:00",
                "agreed_delivery_date": "2024-01-04 13:00:00",
            },
            {
                "created_at": "2024-01-01 10:00:00",
                "last_updated": "2024-01-05 14:00:00",
                "agreed_payment_date": None,
                "agreed_delivery_date": "2024-01-01 15:00:00",
            },
        ]
    }


def test_transform_date_data(mocker, mock_sales_order_data):
    mocker.patch.object(
        IngestionS3Handler,
        "get_data_from_ingestion",
        return_value=mock_sales_order_data,
    )
    test_variable = PandaTransformation()
    df_result = test_variable.transform_date_data()
    expected_list = [
        "2024-01-01 10:00:00",
        "2024-01-02 11:00:00",
        "2024-01-03 12:00:00",
        "2024-01-04 13:00:00",
        "2024-01-05 14:00:00",
        "2024-01-01 15:00:00",
    ]
    expected = pd.to_datetime(expected_list)

    assert df_result["date_id"].isin(expected).all()

    print(df_result.columns.to_list())

    expected_columns = [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter",
    ]

    assert df_result.columns.to_list() == expected_columns
