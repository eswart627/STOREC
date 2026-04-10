import os
import pymysql
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_connection():
    try:
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        database = os.getenv("DB_NAME")
        
        return pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database
        )
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        raise e