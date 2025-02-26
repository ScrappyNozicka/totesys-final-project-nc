from src.extract.extract_utils.connection import create_conn
from pprint import pprint
from pg8000.exceptions import DatabaseError
from datetime import timedelta

class ConnectionError(Exception):
    pass

def get_data_from_db(s3_timestamp=None):
    """Handles data extraction from the initial data-source database.

    On reception of the data we pass them on for processing and selection based on the datetime datastamp.

    Args:
        database connection:
            import database connection func and database close func from different file provided by other members of the team
        S3 timestamp:
            year to milisecond using datetime 
            default to none

    Return:
        all tables with table name as key
        dictionary format

    Raises:
        ConnectionError: Database connection not available.
    """    
    try:
        db = create_conn()
        table_names = ["counterparty", "currency", "department", "design", "staff", "sales_order", "address", "payment", "purchase_order", "payment_type", "transaction"]
        result = {}
        query_minutes_str = ';'
        if s3_timestamp != None:
            query_minutes_str = f' where created_at > {s3_timestamp} \n'
            f' or last_updated > {s3_timestamp};'
        for table in table_names:
            query_result = db.run("SELECT * FROM table" + query_minutes_str)
            columns = [col['name'] for col in db.columns]
            result[table] = [dict(zip(columns, result)) for result in query_result]
        db.close()
        return result
    except Exception as err:
        print("Error: Database connection not found", err)
        raise ConnectionError