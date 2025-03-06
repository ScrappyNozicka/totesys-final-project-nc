from pg8000.native import Connection
import os
from dotenv import load_dotenv

load_dotenv()

def create_conn():
    return Connection(
        os.environ["DW_USER"], 
        password = os.environ["DW_PASSWORD"], 
        database = os.environ["DW_NAME"], 
        host=os.environ["DW_HOST"],
        port=int(os.environ["DW_PORT"])
)

