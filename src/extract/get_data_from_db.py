from src.extract.connection import create_conn, close_db
from pg8000.exceptions import DatabaseError
from datetime import timedelta, datetime
from dotenv import load_dotenv
from s3_loader import s3_loader
import pandas as pd

load_dotenv()



def get_data_from_db():
    """Handles data extraction from the initial data-source database without filtering."""
    
    try:
        db = create_conn()
        table_names = [
            "counterparty", "currency", "department", "design", "staff",
            "sales_order", "address", "payment", "purchase_order", "payment_type", "transaction"
        ]
        result = {}

        for table in table_names:
            query = f"SELECT * FROM {table};"
            query_result = db.run(query)  
            
            columns = [col["name"] for col in db.columns]  
            result[table] = [dict(zip(columns, row)) for row in query_result]

        db.close()
        return result

    except Exception as err:
        raise ConnectionError(f"Database connection error: {err}")

data = get_data_from_db()
# Example usage
filtered_data = s3_loader(data)
for row in filtered_data:
    print(row) 
