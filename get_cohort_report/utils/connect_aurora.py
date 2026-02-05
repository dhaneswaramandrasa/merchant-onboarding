from contextlib import contextmanager
import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

@contextmanager
def get_connection():
    conn = pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )
    try:
        yield conn
    finally:
        conn.close()
