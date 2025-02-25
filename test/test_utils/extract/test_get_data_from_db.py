from src.extract.extract_utils.get_data_from_db import get_data_from_db
import unittest
from unittest import mock
from unittest.mock import patch
from src.extract.extract_utils.connection import create_conn

class MockingTestTestCase(unittest.TestCase):

    @patch('src.extract.extract_utils.connection.create_conn')
    def test_mock_stubs(self, test_patch):
        test_patch.return_value.run = True 
        test_patch.return_value.run.return_value = """ {"Currency": 
        {"currency_id": 1, 
            "currency_code": "GBP", 
            "created_at": 2022-11-03 14:20:49.962, 
            "last_updated": 2022-11-03 14:20:49.962}, 
        {"currency_id": 2, 
            "currency_code": "USD", 
            "created_at": 2022-11-03 14:20:49.962, 
            "last_updated": 2022-11-03 14:20:49.962}, 
        {"currency_id": 3, 
            "currency_code": "EUR", 
            "created_at": 2022-11-03 14:20:49.962, 
            "last_updated": 2022-11-03 14:20:49.962}
            }
           """
        result = {}
        assert get_data_from_db(test_patch) == result


    # @mock.patch('directory1.script1.pd')  # testing pandas
    # @mock.patch('directory1.script1.pyodbc.connect')  # Mocking connection so nothing sent to the outside
    # def test_pandas_read_sql_called(self, mock_access_database, mock_pd):  # unittest for the implentation of the function
    #     p2ctt_data_frame()
    #     self.assert_True(mock_pd.called)  # Make sure that pandas has been called
    #     self.assertIn(
    #         mock.call('select * from P2CTT_2016_Plus0HHs'), mock_pd.mock_calls
    #     )  # This is to make sure the proper value is sent to pandas. We don't need to unittest that pandas handles the
    #     # information correctly.


#def test_get_data_from_db_raised_error_as_expected_when_no_connection():

#def test_get_data_from_db_return_list():

#def test_get_data_from_db_return_list_of_dictionaries():

#def test_get_data_from_db_return_expected_data():

#def test_get_data_from_db_table_exists():

#def test_get_data_from_db_tables_columns_exists():

#def test_get_data_from_db_table_names_as_expected():