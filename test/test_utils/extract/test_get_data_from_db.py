from src.extract.extract_utils.get_data_from_db import get_data_from_db, ConnectionError
import unittest
from unittest.mock import Mock, patch, MagicMock
import pytest
from pg8000.exceptions import DatabaseError
from src.extract.connection import create_conn
import datetime

class MockingTestTestCase(unittest.TestCase):
    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_all_tables_as_expected(self, mock_create_connection):
        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db
        result = {
            'counterparty': [],
            'currency': [],
            'department': [],
            'design': [],
            'staff': [],
            'sales_order': [],
            'address': [],
            'payment': [],
            'purchase_order': [],
            'payment_type': [],
            'transaction': []
        }
        assert get_data_from_db() == result



    def test_get_data_from_db_with_timestamp(this):
        time_stamp = "2022-11-03 14:20:49.961"
        expected_result = [
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'GBP',
                    'currency_id': 1,
                    'last_updated': '2022-11-03--14-20-49-962',
                    },
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'USD',
                    'currency_id': 2,
                    'last_updated': '2022-11-03--14-20-49-962',
                    },
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'EUR',
                    'currency_id': 3,
                    'last_updated': '2022-11-03--14-20-49-962',
                    }
                ]
        result = get_data_from_db(time_stamp)['currency']
        assert result == expected_result


    def test_get_data_from_db_without_timestamp(this):
        expected_result = [
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'GBP',
                    'currency_id': 1,
                    'last_updated': '2022-11-03--14-20-49-962',
                    },
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'USD',
                    'currency_id': 2,
                    'last_updated': '2022-11-03--14-20-49-962',
                    },
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'EUR',
                    'currency_id': 3,
                    'last_updated': '2022-11-03--14-20-49-962',
                    }
                ]
        result = get_data_from_db()['currency']
        assert result == expected_result


    def test_get_data_from_db_with_timestamp(this):
        time_stamp = "2022-11-03 14:20:49.961"
        expected_result = [
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'GBP',
                    'currency_id': 1,
                    'last_updated': '2022-11-03--14-20-49-962',
                    },
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'USD',
                    'currency_id': 2,
                    'last_updated': '2022-11-03--14-20-49-962',
                    },
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'EUR',
                    'currency_id': 3,
                    'last_updated': '2022-11-03--14-20-49-962',
                    }
                ]
        result = get_data_from_db(time_stamp)['currency']
        assert result == expected_result


    def test_get_data_from_db_with_timestamp_no_data(this):
        time_stamp = "2022-11-03 14:20:49.963"
        expected_result = []
        result = get_data_from_db(time_stamp)['currency']
        assert result == expected_result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_address_table_values_as_expected(self, mock_create_connection):
        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time = datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)
        
        mock_connection_db.run.return_value = [
            (1, '6826 Herzog Via', '','Avon', 'New Patienceburgh', '28441', 'Turkey', 1803637401, mock_time, mock_time),
            (2, '179 Alexie Cliffs ', '', '', 'Aliso Viejo', '99305-7380', 'San Marino', 9621880720, mock_time, mock_time),
            (3, '148 Sincere Fort', '', '', 'Lake Charles', '89360', 'Samoa', 1730783349, mock_time, mock_time)
        ]

        mock_connection_db.columns = [
            {'name': 'address_id'},
            {'name': 'address_line_1'},
            {'name': 'address_line_2'},
            {'name': 'district'},
            {'name': 'city'},
            {'name': 'postal_code'},
            {'name': 'country'},
            {'name': 'phone'},
            {'name': 'created_at'},
            {'name': 'last_updated'}
        ]

        result = [{
            'address_id': 1,
            'address_line_1': '6826 Herzog Via',
            'address_line_2': '',
            'city': 'New Patienceburgh',
            'country': 'Turkey',
            'created_at': '2022-11-03--14-20-49-962',
            'district': 'Avon',
            'last_updated': '2022-11-03--14-20-49-962',
            'phone': 1803637401,
            'postal_code': '28441',
            },
            {
            'address_id': 2,
            'address_line_1': '179 Alexie Cliffs ',
            'address_line_2': '',
            'city': 'Aliso Viejo',
            'country': 'San Marino',
            'created_at': '2022-11-03--14-20-49-962',
            'district': '',
            'last_updated': '2022-11-03--14-20-49-962',
            'phone': 9621880720,
            'postal_code': '99305-7380',
            },
            {
            'address_id': 3,
            'address_line_1': '148 Sincere Fort',
            'address_line_2': '',
            'city': 'Lake Charles',
            'country': 'Samoa',
            'created_at': '2022-11-03--14-20-49-962',
            'district': '',
            'last_updated': '2022-11-03--14-20-49-962',
            'phone': 1730783349,
            'postal_code': '89360',
            }
            ]
            
        assert get_data_from_db()["address"] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_counterparty_table_values_as_expected(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time_02 = datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)

        mock_connection_db.run.return_value = [
            (1, 'Fahey and Sons', 15, 'Micheal Toy', 'Mrs. Lucy Runolfsdottir', mock_time_02, mock_time_02),
            (2, 'Leannon, Predovic and Morar', 28, 'Melba Sanford', 'Jean Hane III', mock_time_02, mock_time_02),
            (3, 'Armstrong Inc', 2, 'Jane Wiza', 'Myra Kovacek', mock_time_02, mock_time_02)
        ]  

        mock_connection_db.columns = [
            {'name': 'counterparty_id'},
            {'name': 'counterparty_legal_name'},
            {'name': 'legal_address_id'},
            {'name': 'commercial_contact'},
            {'name': 'delivery_contact'},
            {'name': 'created_at'},
            {'name': 'last_updated'}
        ]

        result = [{
            'counterparty_id': 1,
            'counterparty_legal_name': 'Fahey and Sons',
            'legal_address_id': 15,
            'commercial_contact': 'Micheal Toy',
            'delivery_contact': 'Mrs. Lucy Runolfsdottir',
            'created_at': '2022-11-03--14-20-51-563',
            'last_updated': '2022-11-03--14-20-51-563'
        }, {
            'counterparty_id': 2,
            'counterparty_legal_name': 'Leannon, Predovic and Morar',
            'legal_address_id': 28,
            'commercial_contact': 'Melba Sanford',
            'delivery_contact': 'Jean Hane III',
            'created_at': '2022-11-03--14-20-51-563',
            'last_updated': '2022-11-03--14-20-51-563'
        }, {
            'counterparty_id': 3,
            'counterparty_legal_name': 'Armstrong Inc',
            'legal_address_id': 2,
            'commercial_contact': 'Jane Wiza',
            'delivery_contact': 'Myra Kovacek',
            'created_at': '2022-11-03--14-20-51-563',
            'last_updated': '2022-11-03--14-20-51-563'
        }]

        assert get_data_from_db()['counterparty'] == result
    

    @patch("src.extract.extract_utils.get_data_from_db.create_conn")    
    def test_get_data_from_db_return_currency_table_values_as_expected(self, mock_create_connection):
        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time = datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)
        
        mock_connection_db.run.return_value = [
            [1, 'GBP', mock_time, mock_time],
            [2, 'USD', mock_time, mock_time],
            [3, 'EUR', mock_time, mock_time]
        ]

        mock_connection_db.columns = [
            {'name': 'currency_id'},
            {'name': 'currency_code'},
            {'name': 'created_at'},
            {'name': 'last_updated'}
        ]

        result = [
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'GBP',
                    'currency_id': 1,
                    'last_updated': '2022-11-03--14-20-49-962',
                    },
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'USD',
                    'currency_id': 2,
                    'last_updated': '2022-11-03--14-20-49-962',
                    },
                {
                    'created_at': '2022-11-03--14-20-49-962',
                    'currency_code': 'EUR',
                    'currency_id': 3,    
                    'last_updated': '2022-11-03--14-20-49-962',
                    }
                ]
            
        assert get_data_from_db()['currency'] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_department_table_values_as_expected(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time = datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)

        mock_connection_db.run.return_value = [
            (1, 'Sales', 'Manchester', 'Richard Roma', mock_time, mock_time),
            (2, 'Purchasing', 'Manchester', 'Naomi Lapaglia', mock_time, mock_time),
            (3, 'Production', 'Leeds', 'Chester Ming', mock_time, mock_time)
        ]

        mock_connection_db.columns = [
            {'name': 'department_id'},
            {'name': 'department_name'},
            {'name': 'location'},
            {'name': 'manager'},
            {'name': 'created_at'},
            {'name': 'last_updated'}
        ]

        result = [{
            'department_id': 1,
            'department_name': 'Sales',
            'location': 'Manchester',
            'manager': 'Richard Roma',
            'created_at': '2022-11-03--14-20-49-962',
            'last_updated': '2022-11-03--14-20-49-962'
        }, {
            'department_id': 2,
            'department_name': 'Purchasing',
            'location': 'Manchester',
            'manager': 'Naomi Lapaglia',
            'created_at': '2022-11-03--14-20-49-962',
            'last_updated': '2022-11-03--14-20-49-962'
        }, {
            'department_id': 3,
            'department_name': 'Production',
            'location': 'Leeds',
            'manager': 'Chester Ming',
            'created_at': '2022-11-03--14-20-49-962',
            'last_updated': '2022-11-03--14-20-49-962'
        }]

        assert get_data_from_db()['department'] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_design_table_values_as_expected(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time_01 = datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)
        mock_time_02 = datetime.datetime(2023, 1, 12, 18, 50, 9, 935000)
        mock_time_03 = datetime.datetime(2023, 2, 7, 17, 31, 10, 93000)       

        mock_connection_db.run.return_value = [
            (8, mock_time_01, 'Wooden', '/usr', 'wooden-20220717-npgz.json', mock_time_01),
            (51, mock_time_02, 'Bronze', '/private', 'bronze-20221024-4dds.json', mock_time_02),
            (69, mock_time_03, 'Bronze', '/lost+found', 'bronze-20230102-r904.json', mock_time_03)
        ]

        mock_connection_db.columns = [
            {'name': 'design_id'},
            {'name': 'created_at'},
            {'name': 'design_name'},
            {'name': 'file_location'},
            {'name': 'file_name'},
            {'name': 'last_updated'}
        ]

        result = [{
            'design_id': 8,
            'created_at': '2022-11-03--14-20-49-962',
            'design_name': 'Wooden',
            'file_location': '/usr',
            'file_name': 'wooden-20220717-npgz.json',
            'last_updated': '2022-11-03--14-20-49-962'
        }, {
            'design_id': 51,
            'created_at': '2023-01-12--18-50-09-935',
            'design_name': 'Bronze',
            'file_location': '/private',
            'file_name': 'bronze-20221024-4dds.json',
            'last_updated': '2023-01-12--18-50-09-935'
        }, {
            'design_id': 69,
            'created_at': '2023-02-07--17-31-10-093',
            'design_name': 'Bronze',
            'file_location': '/lost+found',
            'file_name': 'bronze-20230102-r904.json',
            'last_updated': '2023-02-07--17-31-10-093'
        }]

        assert get_data_from_db()['design'] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_payment_table_values_as_expected(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time_02 = datetime.datetime(2022, 11, 3, 14, 20, 52, 187000)
        mock_time_03 = datetime.datetime(2022, 11, 3, 14, 20, 52, 186000)

        mock_connection_db.run.return_value = [
            (2, mock_time_02, mock_time_02, 2, 15, 552548.62, 2, 3, 'f', '2022-11-04', 67305075, 31622269),
            (3, mock_time_03, mock_time_03, 3, 18, 205952.22, 3, 1, 'f', '2022-11-03', 81718079, 47839086),
            (5, mock_time_02, mock_time_02, 5, 17, 57067.20, 2, 3, 'f', '2022-11-06', 66213052, 91659548)
        ]

        mock_connection_db.columns = [
            {'name': 'payment_id'},
            {'name': 'created_at'},
            {'name': 'last_updated'},
            {'name': 'transaction_id'},
            {'name': 'counterparty_id'},
            {'name': 'payment_amount'},
            {'name': 'currency_id'},
            {'name': 'payment_type_id'},
            {'name': 'paid'},
            {'name': 'payment_date'},
            {'name': 'company_ac_number'},
            {'name': 'counterparty_ac_number'}
        ]

        result = [
            {
                'payment_id': 2,
                'created_at': '2022-11-03--14-20-52-187',
                'last_updated': '2022-11-03--14-20-52-187',
                'transaction_id': 2,
                'counterparty_id': 15,
                'payment_amount': 552548.62,
                'currency_id': 2,
                'payment_type_id': 3,
                'paid': 'f',
                'payment_date': '2022-11-04',
                'company_ac_number': 67305075,
                'counterparty_ac_number': 31622269
            },
            {
                'payment_id': 3,
                'created_at': '2022-11-03--14-20-52-186',
                'last_updated': '2022-11-03--14-20-52-186',
                'transaction_id': 3,
                'counterparty_id': 18,
                'payment_amount': 205952.22,
                'currency_id': 3,
                'payment_type_id': 1,
                'paid': 'f',
                'payment_date': '2022-11-03',
                'company_ac_number': 81718079,
                'counterparty_ac_number': 47839086
            },
            {
                'payment_id': 5,
                'created_at': '2022-11-03--14-20-52-187',
                'last_updated': '2022-11-03--14-20-52-187',
                'transaction_id': 5,
                'counterparty_id': 17,
                'payment_amount': 57067.20,
                'currency_id': 2,
                'payment_type_id': 3,
                'paid': 'f',
                'payment_date': '2022-11-06',
                'company_ac_number': 66213052,
                'counterparty_ac_number': 91659548
            }
        ]

        assert get_data_from_db()['payment'] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_payment_type_table_values_as_expected(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time = datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)

        mock_connection_db.run.return_value = [
            (1, 'SALES_RECEIPT', mock_time, mock_time),
            (2, 'SALES_REFUND', mock_time, mock_time),
            (3, 'PURCHASE_PAYMENT', mock_time, mock_time)
        ]

        mock_connection_db.columns = [
            {'name': 'payment_type_id'},
            {'name': 'payment_type_name'},
            {'name': 'created_at'},
            {'name': 'last_updated'}
        ]

        result = [
            {
                'payment_type_id': 1,
                'payment_type_name': 'SALES_RECEIPT',
                'created_at': '2022-11-03--14-20-49-962',
                'last_updated': '2022-11-03--14-20-49-962'
            },
            {
                'payment_type_id': 2,
                'payment_type_name': 'SALES_REFUND',
                'created_at': '2022-11-03--14-20-49-962',
                'last_updated': '2022-11-03--14-20-49-962'
            },
            {
                'payment_type_id': 3,
                'payment_type_name': 'PURCHASE_PAYMENT',
                'created_at': '2022-11-03--14-20-49-962',
                'last_updated': '2022-11-03--14-20-49-962'
            }
        ]

        assert get_data_from_db()['payment_type'] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_purchase_order_table_values_as_expected(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time_02 = datetime.datetime(2022, 11, 3, 14, 20, 52, 187000)
        mock_time_03 = datetime.datetime(2022, 11, 3, 14, 20, 52, 186000)

        mock_connection_db.run.return_value = [
            (1, mock_time_02, mock_time_02, 12, 11, 'ZDOI5EA', 371, 361.39, 2, '2022-11-09', '2022-11-07', 6),
            (2, mock_time_03, mock_time_03, 20, 17, 'QLZLEXR', 286, 199.04, 2, '2022-11-04', '2022-11-07', 8),
            (5, mock_time_03, mock_time_03, 18, 2, 'I9MET53', 316, 803.82, 3, '2022-11-10', '2022-11-05', 2)
        ]

        mock_connection_db.columns = [
            {'name': 'purchase_order_id'},
            {'name': 'created_at'},
            {'name': 'last_updated'},
            {'name': 'staff_id'},
            {'name': 'counterparty_id'},
            {'name': 'item_code'},
            {'name': 'item_quantity'},
            {'name': 'item_unit_price'},
            {'name': 'currency_id'},
            {'name': 'agreed_delivery_date'},
            {'name': 'agreed_payment_date'},
            {'name': 'agreed_delivery_location_id'}
        ]

        result = [
            {
                'purchase_order_id': 1,
                'created_at': '2022-11-03--14-20-52-187',
                'last_updated': '2022-11-03--14-20-52-187',
                'staff_id': 12,
                'counterparty_id': 11,
                'item_code': 'ZDOI5EA',
                'item_quantity': 371,
                'item_unit_price': 361.39,
                'currency_id': 2,
                'agreed_delivery_date': '2022-11-09',
                'agreed_payment_date': '2022-11-07',
                'agreed_delivery_location_id': 6
            },
            {
                'purchase_order_id': 2,
                'created_at': '2022-11-03--14-20-52-186',
                'last_updated': '2022-11-03--14-20-52-186',
                'staff_id': 20,
                'counterparty_id': 17,
                'item_code': 'QLZLEXR',
                'item_quantity': 286,
                'item_unit_price': 199.04,
                'currency_id': 2,
                'agreed_delivery_date': '2022-11-04',
                'agreed_payment_date': '2022-11-07',
                'agreed_delivery_location_id': 8
            },
            {
                'purchase_order_id': 5,
                'created_at': '2022-11-03--14-20-52-186',
                'last_updated': '2022-11-03--14-20-52-186',
                'staff_id': 18,
                'counterparty_id': 2,
                'item_code': 'I9MET53',
                'item_quantity': 316,
                'item_unit_price': 803.82,
                'currency_id': 3,
                'agreed_delivery_date': '2022-11-10',
                'agreed_payment_date': '2022-11-05',
                'agreed_delivery_location_id': 2
            }
        ]

        assert get_data_from_db()['purchase_order'] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_sales_order_table_values_as_expected(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time_02 = datetime.datetime(2022, 11, 3, 14, 20, 52, 188000)
        mock_time_03 = datetime.datetime(2022, 11, 3, 14, 20, 52, 186000)

        mock_connection_db.run.return_value = [
            (2, mock_time_03, mock_time_03, 3, 19, 8, 42972, 3.94, 2, '2022-11-07', '2022-11-08', 8),
            (3, mock_time_02, mock_time_02, 4, 10, 4, 65839, 2.91, 3, '2022-11-06', '2022-11-07', 19),
            (4, mock_time_02, mock_time_02, 4, 10, 16, 32069, 3.89, 2, '2022-11-05', '2022-11-07', 15)
        ]

        mock_connection_db.columns = [
            {'name': 'sales_order_id'},
            {'name': 'created_at'},
            {'name': 'last_updated'},
            {'name': 'design_id'},
            {'name': 'staff_id'},
            {'name': 'counterparty_id'},
            {'name': 'units_sold'},
            {'name': 'unit_price'},
            {'name': 'currency_id'},
            {'name': 'agreed_delivery_date'},
            {'name': 'agreed_payment_date'},
            {'name': 'agreed_delivery_location_id'}
        ]

        result = [
            {
                'sales_order_id': 2,
                'created_at': '2022-11-03--14-20-52-186',
                'last_updated': '2022-11-03--14-20-52-186',
                'design_id': 3,
                'staff_id': 19,
                'counterparty_id': 8,
                'units_sold': 42972,
                'unit_price': 3.94,
                'currency_id': 2,
                'agreed_delivery_date': '2022-11-07',
                'agreed_payment_date': '2022-11-08',
                'agreed_delivery_location_id': 8
            },
            {
                'sales_order_id': 3,
                'created_at': '2022-11-03--14-20-52-188',
                'last_updated': '2022-11-03--14-20-52-188',
                'design_id': 4,
                'staff_id': 10,
                'counterparty_id': 4,
                'units_sold': 65839,
                'unit_price': 2.91,
                'currency_id': 3,
                'agreed_delivery_date': '2022-11-06',
                'agreed_payment_date': '2022-11-07',
                'agreed_delivery_location_id': 19
            },
            {
                'sales_order_id': 4,
                'created_at': '2022-11-03--14-20-52-188',
                'last_updated': '2022-11-03--14-20-52-188',
                'design_id': 4,
                'staff_id': 10,
                'counterparty_id': 16,
                'units_sold': 32069,
                'unit_price': 3.89,
                'currency_id': 2,
                'agreed_delivery_date': '2022-11-05',
                'agreed_payment_date': '2022-11-07',
                'agreed_delivery_location_id': 15
            }
        ]

        assert get_data_from_db()['sales_order'] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_staff_table_values_as_expected(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time = datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)

        mock_connection_db.run.return_value = [
            (1, 'Jeremie', 'Franey', 2, 'jeremie.franey@terrifictotes.com', mock_time, mock_time),
            (2, 'Deron', 'Beier', 6, 'deron.beier@terrifictotes.com', mock_time, mock_time),
            (3, 'Jeanette', 'Erdman', 6, 'jeanette.erdman@terrifictotes.com', mock_time, mock_time)
        ]

        mock_connection_db.columns = [
            {'name': 'staff_id'},
            {'name': 'first_name'},
            {'name': 'last_name'},
            {'name': 'department_id'},
            {'name': 'email_address'},
            {'name': 'created_at'},
            {'name': 'last_updated'}
        ]

        result = [
            {
                'staff_id': 1,
                'first_name': 'Jeremie',
                'last_name': 'Franey',
                'department_id': 2,
                'email_address': 'jeremie.franey@terrifictotes.com',
                'created_at': '2022-11-03--14-20-51-563',
                'last_updated': '2022-11-03--14-20-51-563'
            },
            {
                'staff_id': 2,
                'first_name': 'Deron',
                'last_name': 'Beier',
                'department_id': 6,
                'email_address': 'deron.beier@terrifictotes.com',
                'created_at': '2022-11-03--14-20-51-563',
                'last_updated': '2022-11-03--14-20-51-563'
            },
            {
                'staff_id': 3,
                'first_name': 'Jeanette',
                'last_name': 'Erdman',
                'department_id': 6,
                'email_address': 'jeanette.erdman@terrifictotes.com',
                'created_at': '2022-11-03--14-20-51-563',
                'last_updated': '2022-11-03--14-20-51-563'
            }
        ]

        assert get_data_from_db()['staff'] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_transaction_table_values_as_expected(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time_02 = datetime.datetime(2022, 11, 3, 14, 20, 52, 186000)
        mock_time_03 = datetime.datetime(2022, 11, 3, 14, 20, 52, 187000)

        mock_connection_db.run.return_value = [
            (1, 'PURCHASE', None, 2, mock_time_02, mock_time_02),
            (2, 'PURCHASE', None, 3, mock_time_03, mock_time_03),
            (3, 'SALE', 1, None, mock_time_02, mock_time_02)
        ]

        mock_connection_db.columns = [
            {'name': 'transaction_id'},
            {'name': 'transaction_type'},
            {'name': 'sales_order_id'},
            {'name': 'purchase_order_id'},
            {'name': 'created_at'},
            {'name': 'last_updated'}
        ]

        result = [
            {
                'transaction_id': 1,
                'transaction_type': 'PURCHASE',
                'sales_order_id': None,
                'purchase_order_id': 2,
                'created_at': '2022-11-03--14-20-52-186',
                'last_updated': '2022-11-03--14-20-52-186'
            },
            {
                'transaction_id': 2,
                'transaction_type': 'PURCHASE',
                'sales_order_id': None,
                'purchase_order_id': 3,
                'created_at': '2022-11-03--14-20-52-187',
                'last_updated': '2022-11-03--14-20-52-187'
            },
            {
                'transaction_id': 3,
                'transaction_type': 'SALE',
                'sales_order_id': 1,
                'purchase_order_id': None,
                'created_at': '2022-11-03--14-20-52-186',
                'last_updated': '2022-11-03--14-20-52-186'
            }
        ]

        assert get_data_from_db()['transaction'] == result


    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_return_dict_and_nested_list(self, mock_create_connection):

        mock_connection_db = MagicMock()
        mock_create_connection.return_value = mock_connection_db

        mock_time = datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)

        mock_connection_db.run.return_value = [
            (1, 'PURCHASE', None, 2, mock_time, mock_time),
            (2, 'PURCHASE', None, 3, mock_time, mock_time)
        ]

        mock_connection_db.columns = [
            {'name': 'transaction_id'},
            {'name': 'transaction_type'},
            {'name': 'sales_order_id'},
            {'name': 'purchase_order_id'},
            {'name': 'created_at'},
            {'name': 'last_updated'}
        ]

        result = get_data_from_db()
        assert isinstance(result, dict)
        assert isinstance(result['address'], list)
        assert isinstance(result['design'], list)
        assert isinstance(result['staff'], list)
   
    @patch("src.extract.extract_utils.get_data_from_db.create_conn")
    def test_get_data_from_db_raised_error_as_expected_when_no_connection(self, mock_create_connection):
        
        mock_connection_db = MagicMock()
        mock_create_connection.return_value = None

        with pytest.raises(ConnectionError):
            get_data_from_db()