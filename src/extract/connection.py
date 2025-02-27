from pg8000.native import Connection
import os
from dotenv import load_dotenv

load_dotenv()


def create_conn():
    return Connection(
        os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
    )


def close_db(conn):
    conn.close()
