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
    dbname=DATABASE_NAME,
    user=DATABASE_USERNAME,
    password=DATABASE_PASSWORD
)

def insert_processed_file(file_name, status):
    """
    Insert a record into the processed_files table.
    """
    try:
        # Create a cursor object using the connection
        cursor = pg_conn.cursor()

        # Define the SQL query to insert a record
        query = """
            INSERT INTO processed_files (file_name, status)
            VALUES (%s, %s);
        """
        # Execute the SQL query with parameters
        cursor.execute(query, (file_name, status))

        # Commit the transaction
        pg_conn.commit()
    except Exception as e:
        print("Error inserting record:", e)
        pg_conn.rollback()
    finally:
        # Close the cursor
        cursor.close()

def query_processed_files():
    """
    Query the processed_files table and return all records.
    """
    try:
        # Create a cursor object using the connection
        cursor = pg_conn.cursor()

        # Define the SQL query to select all records
        query = "SELECT file_name, status FROM processed_files;"

        # Execute the SQL query
        cursor.execute(query)

        # Fetch all results
        results = cursor.fetchall()

        return results
    except Exception as e:
        print("Error querying records:", e)
    finally:
        # Close the cursor
        cursor.close()

if pg_conn:
    print("Connection successful")

    insert_processed_file("smallable_campaign_events_20241210120546_test_1.csv", "processed")
    insert_processed_file("export_20240919154941.zip", "processed")
    insert_processed_file("export_20241210120546.zip", "processed")
    insert_processed_file("export_20241023134353.zip", "processed")
    insert_processed_file("smallable_contacts_20240923.csv", "processed")

    results = query_processed_files()
    print("Queried records from processed_files table:")
    for row in results:
        print(row)

    pg_conn.close()
else:
    print("Connection failed")