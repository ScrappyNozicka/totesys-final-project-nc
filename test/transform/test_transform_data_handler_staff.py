import pytest
import pandas as pd
from src.transform.transform_utils.transform_data_handler import (
    PandaTransformation,
)


@pytest.fixture
def mock_data():
    """Returns fake staff data."""
    mock_data = {
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
        "department_all_data": [
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
    return mock_data


def test_transform_staff_data(mock_data, mocker):
    """Test location data transformation."""
    mocker.patch(
        "src.transform.transform_utils.transform_data_handler"
        ".IngestionS3Handler.get_data_from_ingestion",
        return_value=mock_data,
    )
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
