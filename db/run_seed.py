'''This module contains the logic to seed the development databases
for the `Cat's Rare Treasures` FastAPI app.'''
from seed import seed_db

import os

try:
    seed_db()

except Exception as e:
    print(e)
    raise e
