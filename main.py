import os
import time
import csv
import shutil
import logging
from dotenv import load_dotenv

import clickhouse_connect
import boto3

from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.exceptions import ClickHouseError

from util import data_processing, data_pushing, cleanup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

UPLOAD_FOLDER = 'data/uploads/'
EXTRACTED_FOLDER = 'data/uploads/extracted'
PROCESSED_FOLDER = 'data/processed'

PROCESSED_FILES = 'processed_files.txt'
TABLE_SCHEMA = 'table_schema.json'
RENAME_MAPPING = 'rename_mapping.json'

CLICKHOUSE_HOST="cd1.biron-analytics.com"
CLICKHOUSE_DATABASE="smallable2_playground"
CLICKHOUSE_USERNAME= os.getenv('CLICKHOUSE_USERNAME')
CLICKHOUSE_PASSWORD= os.getenv('CLICKHOUSE_PASSWORD')

client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, 
                                       port=8443, 
                                       database=CLICKHOUSE_DATABASE, 
                                       username=CLICKHOUSE_USERNAME, 
                                       password=CLICKHOUSE_PASSWORD)

AWS_BUCKET = os.getenv('AWS_BUCKET')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
S3_UPLOAD_FOLDER = 'uploads/'
S3_PROCESSED_FOLDER = 'processed/'

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

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

for folder in [UPLOAD_FOLDER, EXTRACTED_FOLDER, PROCESSED_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)
        logger.info(f"Created directory: {folder}")

def process_file(filename, s3_file_key):
    """
    Process a single file from S3.
    
    Args:
        filename: Name of the file
        s3_file_key: S3 key of the file
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    cleaned_file = None
    
    try:
        # Download file from S3
        logger.info(f"Downloading {filename} to {UPLOAD_FOLDER}...")
        s3_client.download_file(AWS_BUCKET, s3_file_key, file_path)
        logger.info(f"Processing file: {file_path}")

        # Extract if zip file
        if filename.endswith('.zip'):
            try:
                file_path = data_processing.extract_file(file_path, EXTRACTED_FOLDER)
                logger.info(file_path)
                if not file_path:
                    logger.error(f"Extraction failed for {filename}")
                    if pg_conn:
                        data_pushing.insert_processed_file(pg_conn, filename, 'extraction failed')
                    return False
            except Exception as e:
                logger.exception(f"Critical error extracting {filename}: {e}")
                if pg_conn:
                    data_pushing.insert_processed_file(pg_conn, filename, 'critical extraction error')
                return False

        # Process only CSV files
        if not file_path or not file_path.endswith('.csv'):
            logger.warning(f"Skipping: {file_path} is not a valid CSV file after extraction")
            if pg_conn:
                data_pushing.insert_processed_file(pg_conn, filename, 'not a valid CSV file')
            return False

        # Get base table name from filename
        raw_file_name = os.path.splitext(os.path.basename(file_path))[0]

        # Get table schema to determine table name
        table_schema = data_pushing.get_table_schema(table_name=raw_file_name, file_path=TABLE_SCHEMA)
        if not table_schema:
            logger.error(f"No schema found for table {raw_file_name}")
            if pg_conn:
                data_pushing.insert_processed_file(pg_conn, filename, 'no schema found')
            return False

        # Use table name from schema or fallback to raw filename
        table_name = table_schema.get("table_name", raw_file_name)

        # Create table-specific subfolder in PROCESSED_FOLDER
        table_processed_folder = os.path.join(PROCESSED_FOLDER, table_name)
        os.makedirs(table_processed_folder, exist_ok=True)

        cleaned_file = os.path.join(table_processed_folder, f"{raw_file_name}_cleaned.csv")

        # Get column mapping for renaming
        column_names = data_processing.get_rename_columns(table_name=raw_file_name, file_path=RENAME_MAPPING)
        if not column_names:
            logger.error(f"No column mapping found for {raw_file_name}")
            if pg_conn:
                data_pushing.insert_processed_file(pg_conn, filename, 'no column mapping found')
            return False

        # Rename columns
        logger.info(f"Renaming columns in {file_path}...")
        try:
            rename_result = data_processing.rename_column_in_csv(file_path, column_names, cleaned_file)
            if not rename_result.get('success', True):
                logger.error(f"ERROR: {rename_result.get('error', 'Unknown error')}")
                if pg_conn:
                    data_pushing.insert_processed_file(pg_conn, filename, 'rename error')
                return False
        except Exception as e:
            logger.exception(f"Error renaming columns: {e}")
            if pg_conn:
                data_pushing.insert_processed_file(pg_conn, filename, 'critical rename error')
            return False

        # Check for missing columns
        logger.info(f"Checking for missing columns in {cleaned_file}...")

        expected_columns = table_schema.get("column_names", [])

        # Read current columns
        encoding = data_processing.detect_encoding(cleaned_file)
        with open(cleaned_file, 'r', newline='', encoding=encoding) as csvfile:
            reader = csv.reader(csvfile)
            current_columns = next(reader)  # Get header row

        # Add missing columns
        if len(current_columns) + 2 != len(expected_columns):  # +2 for 'id' column and 'row_hash' column
            logger.error(f"Column count mismatch: {len(current_columns) + 2} found, {len(expected_columns)} expected.")
            if pg_conn:
                data_pushing.insert_processed_file(pg_conn, filename, 'column count mismatch')
            return False
        else:
            logger.info(f"Column count matches: {len(current_columns) + 2} found, {len(expected_columns)} expected.")

        for column_name in expected_columns:
            if column_name not in current_columns and column_name != 'id' and column_name != 'row_hash':
                position = expected_columns.index(column_name)
                logger.info(f"Adding missing column {column_name} to {cleaned_file}...")
                temp_file = f"{os.path.splitext(cleaned_file)[0]}.temp.csv"
                try:
                    add_column_result = data_processing.add_column_to_csv(cleaned_file, temp_file, column_name, position)
                    if not add_column_result.get('success', True):
                        logger.error(f"ERROR: {add_column_result.get('error', 'Unknown error')}")
                        if pg_conn:
                            data_pushing.insert_processed_file(pg_conn, filename, 'add column error')
                        return False
                except Exception as e:
                    logger.exception(f"Error adding column {column_name}: {e}")
                    if pg_conn:
                        data_pushing.insert_processed_file(pg_conn, filename, 'critical add column error')
                    return False
                shutil.move(temp_file, cleaned_file)

        # Remove duplicates within the file
        logger.info(f"Checking for duplicates in {cleaned_file}...")
        try:
            self_deduplicate_result = data_processing.self_deduplicate_csv(cleaned_file, cleaned_file)
            if not self_deduplicate_result.get('success', True):
                logger.error(f"ERROR: {self_deduplicate_result.get('error', 'Unknown error')}")
                if pg_conn:
                    data_pushing.insert_processed_file(pg_conn, filename, 'self deduplication error')
                return False
        except Exception as e:
            logger.exception(f"Error during deduplication: {e}")
            if pg_conn:
                data_pushing.insert_processed_file(pg_conn, filename, 'critical self deduplication error')
            return False

        # Mark file as processed
        if pg_conn:
            data_pushing.insert_processed_file(pg_conn, filename, 'processed')
            logger.info(f"Inserted {filename} into processed_files table.")

        logger.info('Checking for processed files...')

        # Get all existing files in the table-specific subfolder
        existing_files = [
            f for f in os.listdir(table_processed_folder)
            if os.path.isfile(os.path.join(table_processed_folder, f))
            and f != os.path.basename(cleaned_file)
        ]

        if len(existing_files) >= 1:  # At least 1 other file to compare with
            logger.info("Performing cross-file comparison...")
            for prev_file in existing_files:
                prev_file_path = os.path.join(table_processed_folder, prev_file)
                try:
                    logger.info(f"Comparing {cleaned_file} with {prev_file_path}...")
                    # Create a new dedup filename
                    dedup_file_path = os.path.join(table_processed_folder, f"dedup_{os.path.basename(cleaned_file)}")

                    compare_and_deduplicate_result = data_processing.compare_and_deduplicate_csv_files(
                        prev_file_path,
                        cleaned_file,
                        dedup_file_path
                    )

                    if not compare_and_deduplicate_result.get('success', True):
                        logger.error(f"ERROR: {compare_and_deduplicate_result.get('error', 'Unknown error')}")
                        if pg_conn:
                            data_pushing.insert_processed_file(pg_conn, filename, 'cross-file comparison error')
                        return False

                    # Then optionally replace the cleaned file if needed
                    shutil.move(dedup_file_path, cleaned_file)
                except Exception as e:
                    logger.exception(f"Error comparing {cleaned_file} and {prev_file_path}: {e}")
                    if pg_conn:
                        data_pushing.insert_processed_file(pg_conn, filename, 'critical cross-file comparison error')
                    return False
        else:
            logger.info("No other files to compare with. Proceeding to insert.")

        # Insert into ClickHouse
        last_id = table_schema.get("last_id", 0)
        date_columns = table_schema.get("date_columns", [])
        int_columns = table_schema.get("int_columns", [])
        float_columns = table_schema.get("float_columns", [])
        string_columns = table_schema.get("string_columns", [])
        dob_columns = table_schema.get("dob_columns", [])
        column_names = table_schema.get("column_names", [])
        column_types = table_schema.get("column_types", [])

        logger.info(f"Pushing {cleaned_file} to ClickHouse...")
        try:
            process_and_insert_result = data_pushing.process_and_insert_csv(
                client,
                table_name,
                cleaned_file,
                chunk_size=10000,
                last_id=last_id,
                date_columns=date_columns,
                int_columns=int_columns,
                float_columns=float_columns,
                string_columns=string_columns,
                dob_columns=dob_columns,
                column_names=column_names,
                column_type_names=column_types,
            )
            if not process_and_insert_result.get('success', True):
                logger.error(f"ERROR: {process_and_insert_result.get('error', 'Unknown error')}")
                if pg_conn:
                    data_pushing.insert_processed_file(pg_conn, filename, 'insert error')
                return False
        except Exception as e:
            logger.exception(f"Error inserting {cleaned_file} into ClickHouse: {e}")
            if pg_conn:
                data_pushing.insert_processed_file(pg_conn, filename, 'critical insert error')
            return False

        logger.info(f"Successfully uploaded {cleaned_file} to ClickHouse")

        # Move the processed file to the processed folder in S3
        try:
            # Ensure the folder structure exists in S3 before uploading
            try:
                s3_client.put_object(Bucket=AWS_BUCKET, Key=f"{S3_PROCESSED_FOLDER}/{table_name}/")
            except Exception as e:
                logger.exception(f"Error creating folder structure in S3: {e}")
                return False

            # Upload the file to S3
            s3_client.upload_file(cleaned_file, AWS_BUCKET, f"{S3_PROCESSED_FOLDER}/{table_name}/{os.path.basename(cleaned_file)}")
            logger.info(f"Uploaded {cleaned_file} to S3 bucket {AWS_BUCKET}/{S3_PROCESSED_FOLDER}")
        except Exception as e:
            logger.exception(f"Error uploading {cleaned_file} to S3: {e}")
            if pg_conn:
                data_pushing.insert_processed_file(pg_conn, filename, 'upload error')
            return False

        # Update last_id
        logger.info(f"Updating last_id for {table_name} ...")
        try:
            update_last_id_result = data_pushing.update_last_id(client, table_name, TABLE_SCHEMA)
            if not update_last_id_result.get('success', True):
                logger.error(f"ERROR: {update_last_id_result.get('error', 'Unknown error')}")
                if pg_conn:
                    data_pushing.insert_processed_file(pg_conn, filename, 'update last_id error')
                return False
        except Exception as e:
            logger.exception(f"Error updating last_id: {e}")
            return False

        return True
        
    except Exception as e:
        logger.exception(f"Unexpected error processing file {filename}: {e}")
        if pg_conn:
            data_pushing.insert_processed_file(pg_conn, filename, f'unexpected error: {str(e)[:100]}')
        return False
        
    finally:
        # Clean up all temporary files whether processing succeeded or failed
        cleanup.cleanup_files(file_path)
        # Don't clean up cleaned_file yet as we might need it for future deduplication

def main():
    """
    Process files in the UPLOAD_FOLDER and save processed files in table-specific subfolders
    within the PROCESSED_FOLDER before pushing to ClickHouse.
    Checks daily for new files, skips already processed ones, and handles deduplication
    both within and across files.
    """
    logger.info("Starting the file processing script...")
    
    # Run storage cleanup on startup to ensure we have space
    cleanup.check_storage_and_cleanup()

    try:
        # Query processed files
        query = data_pushing.query_processed_files(pg_conn)
        processed_files = set([file[0] for file in query])

        # List all files in S3 upload folder
        uploaded_files = s3_client.list_objects_v2(Bucket=AWS_BUCKET, Prefix=S3_UPLOAD_FOLDER)
        if 'Contents' not in uploaded_files or not uploaded_files['Contents']:
            logger.info("No new files to process. Exiting...")
            return

        # Check for new files to process
        for obj in uploaded_files['Contents']:
            file_key = obj['Key']
            filename = os.path.basename(file_key)

            # Skip if not a file or already processed or wrong format
            if filename in processed_files or not filename.endswith(('.csv', '.zip')):
                logger.info(f"Skipping {filename}: Already processed, not a file, or invalid format.")
                continue
                
            success = False
            try:
                # Process the file
                success = process_file(filename, file_key)
                
                if success:
                    logger.info(f"Successfully processed file: {filename}")
                    # Check storage after each successful processing
                    cleanup.check_storage_and_cleanup()
                else:
                    logger.error(f"Failed to process file: {filename}")
            except Exception as e:
                logger.exception(f"Unexpected error processing file {filename}: {e}")
            finally:
                # Clean up files in the UPLOAD_FOLDER regardless of processing outcome
                upload_file_path = os.path.join(UPLOAD_FOLDER, filename)
                cleanup.cleanup_files(upload_file_path)
                
                # Clean up extracted files
                if filename.endswith('.zip'):
                    # Get base filename without extension
                    base_name = os.path.splitext(filename)[0]
                    # Clean up any potential extracted files with similar names in the extracted folder
                    for extracted_file in os.listdir(EXTRACTED_FOLDER):
                        if extracted_file.startswith(base_name):
                            cleanup.cleanup_files(os.path.join(EXTRACTED_FOLDER, extracted_file))
    
    except Exception as e:
        logger.exception(f"Unexpected error in main function: {e}")
    
    finally:
        # Always run cleanup before exiting
        cleanup.cleanup_old_files(UPLOAD_FOLDER)
        cleanup.cleanup_old_files(EXTRACTED_FOLDER)
        
        # Only clean up old processed files, not all of them
        # as we need some for deduplication
        cleanup.cleanup_old_files(PROCESSED_FOLDER, max_age_days=7)  # Keep processed files a bit longer
        
        # Final storage check
        cleanup.check_storage_and_cleanup()
        
        logger.info("File processing completed.")

if __name__ == '__main__':
    # Run storage cleanup on startup to ensure we have space
    logger.info("Cleaning up old files...")
    cleanup.cleanup_old_files(UPLOAD_FOLDER, max_age_days=1)
    cleanup.cleanup_old_files(EXTRACTED_FOLDER, max_age_days=1)
    cleanup.cleanup_old_files(PROCESSED_FOLDER, max_age_days=7)

    while True:
        logger.info("Running the main function...")
        try:
            main()
        except Exception as e:
            logger.exception(f"Unexpected error in main function: {e}")
        logger.info("Sleeping for 7 days...")
        time.sleep(7 * 24 * 60 * 60)  # Sleep for 7 days