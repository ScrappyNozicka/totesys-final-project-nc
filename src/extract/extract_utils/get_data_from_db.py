from connection import create_conn
import logging


class ConnectionError(Exception):
    pass


CREATED_OR_UPDATED_AFTER_SQL = """
    (created_at > '{from_timestamp}' OR last_updated > '{from_timestamp}')
"""
CREATED_OR_UPDATED_BEFORE_SQL = """
    (created_at <= '{to_timestamp}' AND last_updated <= '{to_timestamp}')
"""
SELECT_FROM_TABLE_SQL = "SELECT * FROM {table_name} {where};"


def get_data_from_db(
    from_timestamp: str, to_timestamp: str
) -> dict[list[dict]]:
    """Handles data extraction from the initial data-source database.
    On reception of the data we pass them on for processing.
    Selection based on the datetime datastamp initiated.
    Args:
        database connection:
            import database connection func
            import database close func
        from_timestamp:
            timestamp from which data should be filtered
        to_timestamp:
            timestamp to which data should be filtered
    Return:
        all tables with table name as key
        dictionary format
    Raises:
        ConnectionError: Database connection not available.
    """

    result = {}
    db = None

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

        conditions = []
        if from_timestamp:
            conditions.append(
                CREATED_OR_UPDATED_AFTER_SQL.format(
                    from_timestamp=from_timestamp
                )
            )
        if to_timestamp:
            conditions.append(
                CREATED_OR_UPDATED_BEFORE_SQL.format(to_timestamp=to_timestamp)
            )
        where_clause = (
            f"WHERE {' AND '.join(conditions)}" if conditions else ""
        )

        for table_name in table_names:
            query_str = SELECT_FROM_TABLE_SQL.format(
                table_name=table_name, where=where_clause
            )
            query_result = db.run(query_str)
            columns = [col["name"] for col in db.columns]
            table_data = [
                dict(zip(columns, result)) for result in query_result
            ]

            result[table_name] = table_data
            logging.info(f"Data retrieved for table: {table_name}")
    except Exception as err:
        logging.error(f"Error retrieving data from database: {err}")
        raise ConnectionError

    finally:
        if db:
            db.close()
            logging.info("Database connection closed.")

    return result
