import os
from src.extract.connection import create_conn


def test_create_conn(mocker):
    """Test create_conn creates a Connection with correct parameters."""
    # Mock environment variables
    os.environ["DB_USER"] = "test_user"
    os.environ["DB_PASSWORD"] = "test_password"
    os.environ["DB_NAME"] = "test_db"
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5432"

    mock_connection = mocker.patch("src.extract.connection.Connection")

    create_conn()

    mock_connection.assert_called_once_with(
        "test_user",
        password="test_password",
        database="test_db",
        host="localhost",
        port=5432,
    )
