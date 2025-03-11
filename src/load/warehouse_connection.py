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