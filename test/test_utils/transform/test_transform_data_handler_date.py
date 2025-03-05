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
# @pytest.fixture
def test_transform_date_data():
    """Test date data transformation."""
    test_variable = PandaTransformation()
    df_result = test_variable.transform_date_data()
    date_range = pd.date_range(start="2022-01-01", end="2047-12-31", freq="D")
    expected_df = pd.DataFrame(
        {
            "date_id": date_range,
            "year": date_range.year,
            "month": date_range.month,
            "day": date_range.day,
            "day_of_week": date_range.dayofweek,
            "day_name": date_range.strftime("%A"),
            "month_name": date_range.strftime("%B"),
            "quarter": date_range.quarter,
        }
    )
    pd.testing.assert_frame_equal(df_result, expected_df)