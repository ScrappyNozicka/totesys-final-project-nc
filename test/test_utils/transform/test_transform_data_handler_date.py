import pytest
import pandas as pd
from src.transform.transform_utils.transform_data_handler import (
    PandaTransformation,
)


@pytest.fixture
def transformator():
    return PandaTransformation()


def test_transform_date_data_range(transformator):
    df_result = transformator.transform_date_data()

    assert df_result["date_id"].iloc[0] == pd.to_datetime("2022-01-01")
    assert df_result["date_id"].iloc[-1] == pd.to_datetime("2047-12-31")


def test_transform_date_data_valid_columns(transformator):
    df_result = transformator.transform_date_data()
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


def test_transform_date_data_random_id_in_range(transformator):
    df_result = transformator.transform_date_data()
    expected_list = [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
    ]
    expected = pd.to_datetime(expected_list)

    assert df_result["date_id"].isin(expected).any()
