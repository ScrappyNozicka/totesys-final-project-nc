from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


def create_conn():
    """Creates and returns a database engine using SQLAlchemy"""
    user = os.getenv("DW_USER")
    password = os.getenv("DW_PASSWORD")
    host = os.getenv("DW_HOST")
    dbname = os.getenv("DW_NAME")
    port = os.getenv("DW_PORT")

    
    db_url = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'

    return create_engine(db_url)



# db = create_conn()

# with db.connect() as conn:
#     conn.execute(text("DELETE FROM fact_sales_order"))
#     conn.execute(text("DELETE FROM dim_currency"))
#     conn.execute(text("DELETE FROM dim_date"))
#     conn.execute(text("DELETE FROM dim_design"))
#     conn.execute(text("DELETE FROM dim_location"))
#     conn.execute(text("DELETE FROM dim_staff"))
#     conn.execute(text("DELETE FROM dim_counterparty"))

#     conn.commit() 

    #result = conn.execute(text("SELECT * FROM dim_date"))
    
    #print(result.fetchall()) 
    
