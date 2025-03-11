import pytest
import pandas as pd
from src.transform.transform_utils.transform_data_handler import (
    PandaTransformation,
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


def test_returns_dictionary_of_dataframes_even_when_one_is_empty(
    mocker, mock_sales_order_data
):
    """Test function returns dictionary even if one dataframe is empty."""
    mocker.patch.object(
        PandaTransformation,
        "transform_currency_data",
        return_value=pd.DataFrame(mock_sales_order_data),
    )
    mocker.patch.object(
        PandaTransformation,
        "transform_location_data",
        return_value=pd.DataFrame(mock_sales_order_data),
    )
    mocker.patch.object(
        PandaTransformation,
        "transform_staff_data",
        return_value=pd.DataFrame(mock_sales_order_data),
    )
    mocker.patch.object(
        PandaTransformation,
        "transform_design_data",
        return_value=pd.DataFrame(mock_sales_order_data),
    )
    mocker.patch.object(
        PandaTransformation,
        "transform_counterparty_data",
        return_value=pd.DataFrame(mock_sales_order_data),
    )
    mocker.patch.object(
        PandaTransformation,
        "transform_date_data",
        return_value=pd.DataFrame(mock_sales_order_data),
    )
    mocker.patch.object(
        PandaTransformation,
        "transform_sales_order_data",
        return_value=pd.DataFrame(mock_sales_order_data),
    )
    mocker.patch.object(
        PandaTransformation,
        "check_date_file_exists",
        return_value=False,
    )

    test_instance = PandaTransformation()
    result = test_instance.returns_dictionary_of_dataframes()

    expected_keys = [
        "dim_currency",
        "dim_location",
        "dim_staff",
        "dim_design",
        "dim_counterparty",
        "fact_sales_order",
        "dim_date",
    ]

    assert len(result.keys()) == 7
    assert list(result.keys()) == expected_keys
