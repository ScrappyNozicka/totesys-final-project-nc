import pytest
import json
import os
import pandas as pd
from unittest.mock import MagicMock 

from src.transform.transform_utils.transform_data_handler import PandaTransformation


@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""

    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


# def test_transform_data_handler():
#     pass


def test_transform_currency_data_returns_file():
    result_df = PandaTransformation
    result_data = result_df.transform_currency_data()
    assert result_data["currency_name"].iloc[0] == "Great Britain Pound"


#test input file valid format
#test input file invalid format


#test input file valid name
#test input file invalid name

#test return expected format
#test return value

#test output file valid name
#test output file invalid name
#test output file valid format
#test output file invalid format

#test currency retun expected value
#test currency return expected columns
#test currency return expected types
#test currency return expected length