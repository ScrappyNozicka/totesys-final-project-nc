import pytest
from src.extract.extract_utils.totesys_processor import ToteSysProcessor


@pytest.fixture
def sample_data():
    return {
        "orders": [
            {"orders_id": "123", "last_updated": "2024-02-25"},
            {"orders_id": "124", "last_updated": "2024-02-26"},
        ]
    }


def test_get_table_names(sample_data):
    processor = ToteSysProcessor()
    result = processor.get_table_names(sample_data)
    assert result == ["orders"]


def test_get_row_id(sample_data):
    processor = ToteSysProcessor()
    row = sample_data["orders"][0]
    table_name = "orders"
    result = processor.get_row_id(row, table_name)
    assert result == "123"


def test_get_last_updated(sample_data):
    processor = ToteSysProcessor()
    row = sample_data["orders"][0]
    result = processor.get_last_updated(row)
    assert result == "2024-02-25"
