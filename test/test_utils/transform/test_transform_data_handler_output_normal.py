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


@pytest.fixture
def mock_currency_lookup(mocker):
    """Mock JSON currency lookup file."""
    mocker.patch(
        "builtins.open",
        mocker.mock_open(read_data='{"USD": "United States Dollar", "EUR": "Euro"}'),
    )
    mocker.patch(
        "json.load", return_value={"USD": "United States Dollar", "EUR": "Euro"}
    )


@pytest.fixture
def mock_ingestion_s3_handler(mocker, mock_currency_lookup):
    """Mock IngestionS3Handler to return fake data."""
    mock_handler = mocker.patch(
        "src.transform.transform_utils.transform_data_handler.IngestionS3Handler"
    )
    mock_instance = mock_handler.return_value
    mock_instance.get_data_from_ingestion.return_value = {
        "currency": [
            {
                "currency_code": "USD",
                "created_at": "2024-03-01",
                "last_updated": "2024-03-05",
            },
            {
                "currency_code": "EUR",
                "created_at": "2024-03-01",
                "last_updated": "2024-03-05",
            },
        ],
        "address": [
            {
                "address_id": 1,
                "address_line_1": "6826 Herzog Via",
                "address_line_2": "",
                "district": "Avon",
                "city": "New Patienceburgh",
                "postal_code": "28441",
                "country": "Turkey",
                "phone": "1803 637401",
                "created_at": "2022-11-03 14:20:49.962",
                "last_updated": "2022-11-03 14:20:49.962",
            },
            {
                "address_id": 2,
                "address_line_1": "179 Alexie Cliffs",
                "address_line_2": "",
                "district": "",
                "city": "Aliso Viejo",
                "postal_code": "99305-7380",
                "country": "San Marino",
                "phone": "9621 880720",
                "created_at": "2022-11-03 14:20:49.962",
                "last_updated": " 2022-11-03 14:20:49.962",
            },
        ],
        "address_all_data": [
            {
                "address_id": 1,
                "address_line_1": "6826 Herzog Via",
                "address_line_2": "",
                "district": "Avon",
                "city": "New Patienceburgh",
                "postal_code": "28441",
                "country": "Turkey",
                "phone": "1803 637401",
                "created_at": "2022-11-03 14:20:49.962",
                "last_updated": "2022-11-03 14:20:49.962",
            },
            {
                "address_id": 2,
                "address_line_1": "179 Alexie Cliffs",
                "address_line_2": "",
                "district": "",
                "city": "Aliso Viejo",
                "postal_code": "99305-7380",
                "country": "San Marino",
                "phone": "9621 880720",
                "created_at": "2022-11-03 14:20:49.962",
                "last_updated": " 2022-11-03 14:20:49.962",
            },
        ],
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
        "design": [
            {
                "design_id": 8,
                "created_at": "2022-11-03 14:20:49.962",
                "design_name": "Wooden",
                "file_location": "/usr",
                "file_name": "wooden-20220717-npgz.json",
                "last_updated": "2023-01-12 18:50:09.935",
            },
            {
                "design_id": 51,
                "created_at": "2023-01-12 18:50:09.935",
                "design_name": "Bronze",
                "file_location": "/private",
                "file_name": "bronze-20221024-4dds.json",
                "last_updated": "2023-01-12 18:50:09.935",
            },
        ],
        "counterparty": [
            {
                "counterparty_id": 1,
                "counterparty_legal_name": "Fahey and Sons",
                "legal_address_id": 1,
                "commercial_contact": "Micheal Toy",
                "delivery_contact": "Mrs. Lucy Runolfsdottir",
                "created_at": "2022-11-03 14:20:51.563",
                "last_updated": "2022-11-03 14:20:51.563",
            },
            {
                "counterparty_id": 2,
                "counterparty_legal_name": "Leannon, Predovic and Morar",
                "legal_address_id": 2,
                "commercial_contact": "Melba Sanford",
                "delivery_contact": "Jean Hane III",
                "created_at": "2022-11-03 14:20:51.563",
                "last_updated": "2022-11-03 14:20:51.563",
            },
        ],
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
        ],
    }
    return mock_instance


def test_dictionary_of_dataframes_happy_path(mock_ingestion_s3_handler):
    """Test if function returns dictionary containing all dataframes."""
    test_variable = PandaTransformation()
    result = test_variable.returns_dictionary_of_dataframes()
    expected_currency_df = pd.DataFrame(
        {
            "currency_code": ["USD", "EUR"],
            "currency_name": ["United States Dollar", "Euro"],
        }
    )
    expected_location_df = pd.DataFrame(
        {
            "location_id": [1, 2],
            "address_line_1": ["6826 Herzog Via", "179 Alexie Cliffs"],
            "address_line_2": ["", ""],
            "district": ["Avon", ""],
            "city": ["New Patienceburgh", "Aliso Viejo"],
            "postal_code": ["28441", "99305-7380"],
            "country": ["Turkey", "San Marino"],
            "phone": ["1803 637401", "9621 880720"],
        }
    )
    expected_staff_df = pd.DataFrame(
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
    expected_design_df = pd.DataFrame(
        {
            "design_id": [8, 51],
            "design_name": ["Wooden", "Bronze"],
            "file_location": ["/usr", "/private"],
            "file_name": ["wooden-20220717-npgz.json", "bronze-20221024-4dds.json"],
        }
    )
    expected_counterparty_df = pd.DataFrame(
        {
            "counterparty_id": [1, 2],
            "counterparty_legal_name": [
                "Fahey and Sons",
                "Leannon, Predovic and Morar",
            ],
            "counterparty_legal_address_line_1": [
                "6826 Herzog Via",
                "179 Alexie Cliffs",
            ],
            "counterparty_legal_address_line_2": ["", ""],
            "counterparty_legal_district": ["Avon", ""],
            "counterparty_legal_city": ["New Patienceburgh", "Aliso Viejo"],
            "counterparty_legal_postal_code": ["28441", "99305-7380"],
            "counterparty_legal_country": ["Turkey", "San Marino"],
            "counterparty_legal_phone_number": ["1803 637401", "9621 880720"],
        }
    )
    expected_sales_order_df = pd.DataFrame(
        {
            "sales_record_id": [1, 2],
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
    date_range = pd.date_range(start="2022-01-01", end="2047-12-31", freq="D")
    expected_date_df = pd.DataFrame(
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
    expected = {
        "currency": expected_currency_df,
        "location": expected_location_df,
        "staff": expected_staff_df,
        "design": expected_design_df,
        "counterparty": expected_counterparty_df,
        "sales_order": expected_sales_order_df,
        "date": expected_date_df,
    }
    pd.testing.assert_frame_equal(result["currency"], expected["currency"])
    pd.testing.assert_frame_equal(result["location"], expected["location"])
    pd.testing.assert_frame_equal(result["staff"], expected["staff"])
    pd.testing.assert_frame_equal(result["design"], expected["design"])
    pd.testing.assert_frame_equal(result["counterparty"], expected["counterparty"])
    pd.testing.assert_frame_equal(result["sales_order"], expected["sales_order"])
    pd.testing.assert_frame_equal(result["date"], expected["date"])


# test if all of the tables are empty
