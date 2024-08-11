import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

class PostgresDB:
    def __init__(self):
        self.connection_string = (
            f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} "
            f"password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} "
            f"port={os.getenv('DB_PORT')}"
        )
        self.conn = None

    def connect(self):
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(self.connection_string)
            except psycopg2.DatabaseError as e:
                print(f"Error connecting to the database: {e}")
                raise

    def disconnect(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def execute_query(self, query):
        if self.conn is None:
            raise ConnectionError("Database is not connected")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                colnames = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=colnames)
        except psycopg2.DatabaseError as e:
            print(f"Error executing query: {e}")
            raise
