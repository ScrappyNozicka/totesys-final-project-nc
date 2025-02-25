from src.extract.extract_utils.connection import create_conn, close_conn
from pprint import pprint

def get_data_from_db(query):
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
    db = create_conn()
    table_names = ["counterparty", "currency", "department", "design", "staff", "sales_order", "address", "payment", "purchase_order", "payment_type", "transaction"]
    result = []
    for table in table_names:
        query_result = db.run("SELECT * FROM table;")
        columns = [col['name'] for col in db.columns]
        result.append([dict(zip(columns, result)) for result in query_result])
    db.close()
    pprint(result)
    return result

    #pass in the create connection
    #create list of table names
    #iterate over each table to get the data
    #db run to select all from the table if timestamp none
    #if timestamp, select only after the timestamp
    #pass in the close connection
    #return all the data selected