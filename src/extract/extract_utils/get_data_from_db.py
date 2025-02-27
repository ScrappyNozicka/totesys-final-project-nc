from src.extract.connection import create_conn


class ConnectionError(Exception):
    pass


def get_data_from_db(s3_timestamp=None):
    """Handles data extraction from the initial data-source database.
    On reception of the data we pass them on for processing.
    Selection based on the datetime datastamp initiated.
    Args:
        database connection:
            import database connection func
            import database close func
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
        table_names = [
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
        result = {}
        query_minutes_str = ";"
        if s3_timestamp is not None:
            query_minutes_str = f""" where created_at > '{s3_timestamp}'
                             or last_updated > '{s3_timestamp}';"""
        for table in table_names:
            query_str = f"SELECT * FROM {table}" + query_minutes_str
            query_result = db.run(query_str)
            columns = [col["name"] for col in db.columns]
            table_data = [dict(zip(columns, result)) for result in query_result]
            for item in table_data:
                item["created_at"] = item["created_at"].strftime(
                    "%Y-%m-%d--%H-%M-%S-%f"
                )[:-3]
                item["last_updated"] = item["last_updated"].strftime(
                    "%Y-%m-%d--%H-%M-%S-%f"
                )[:-3]
            result[table] = table_data
        db.close()
        return result
    except Exception as err:
        print("Error: Database connection not found", err)
        raise ConnectionError
