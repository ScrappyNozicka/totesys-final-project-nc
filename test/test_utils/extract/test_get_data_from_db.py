import pytest
from unittest.mock import patch, MagicMock
from src.extract.extract_utils.get_data_from_db import (
    get_data_from_db,
    ConnectionError,
)


# Mock table names as per your actual function
TABLE_NAMES = [
    "counterparty",
    "currency",
    "department",
    "design",
    "staff",
    "sales_order",
    "address",
    "payment",
    "purchase_order",
    "payment_type",
    "transaction",
]


@patch("src.extract.extract_utils.get_data_from_db.create_conn")
def test_get_data_from_db_success(mock_create_conn):
    # Arrange
    mock_db = MagicMock()
    mock_create_conn.return_value = mock_db

    # Simulate columns returned from db
    mock_db.columns = [{"name": "id"}, {"name": "name"}]
    # Simulate data returned from run
    mock_db.run.return_value = [[1, "Example"], [2, "Test"]]

    from_timestamp = "2024-01-01 00:00:00"
    to_timestamp = "2024-12-31 23:59:59"

    # Act
    result = get_data_from_db(from_timestamp, to_timestamp)

    # Assert
    assert isinstance(result, dict)
    assert len(result) == len(TABLE_NAMES)
    for table in TABLE_NAMES:
        assert table in result
        assert result[table] == [
            {"id": 1, "name": "Example"},
            {"id": 2, "name": "Test"},
        ]
    assert mock_db.run.call_count == len(TABLE_NAMES)
    mock_db.close.assert_called_once()


@patch("src.extract.extract_utils.get_data_from_db.create_conn")
def test_get_data_from_db_no_timestamps(mock_create_conn):
    # Arrange
    mock_db = MagicMock()
    mock_create_conn.return_value = mock_db
    mock_db.columns = [{"name": "id"}]
    mock_db.run.return_value = [[1]]

    # Act
    result = get_data_from_db(None, None)

    # Assert
    assert isinstance(result, dict)
    assert len(result) == len(TABLE_NAMES)
    for table in TABLE_NAMES:
        assert table in result
        assert result[table] == [{"id": 1}]
    assert mock_db.run.call_count == len(TABLE_NAMES)
    mock_db.close.assert_called_once()


@patch("src.extract.extract_utils.get_data_from_db.create_conn")
def test_get_data_from_db_connection_error(mock_create_conn):
    # Arrange
    mock_create_conn.side_effect = Exception("DB failure")

    # Act / Assert
    with pytest.raises(ConnectionError):
        get_data_from_db("2024-01-01", "2024-12-31")
