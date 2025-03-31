import os
from util import data_pushing
from dotenv import load_dotenv
load_dotenv()

DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_PORT = os.getenv('DATABASE_PORT')
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_USERNAME = os.getenv('DATABASE_USERNAME')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')

pg_conn = data_pushing.get_postgres_connection(
    host=DATABASE_HOST,
    port=DATABASE_PORT,
    database=DATABASE_NAME,
    user=DATABASE_USERNAME,
    password=DATABASE_PASSWORD
)

def query_processing_status(pg_conn):
    """
    Function to query the processing status from the database.
    """
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute("SELECT file_name, status, created_at FROM processed_files")
            records = cursor.fetchall()
            return records
    except Exception as e:
        print(f"Error querying processing status: {e}")

if pg_conn:
    results = query_processing_status(pg_conn)
    if results:
        for record in results:
            print(f"Filename: {record[0]}, Status: {record[1]}, Created At: {record[2]}")
    else:
        print("No records found.")