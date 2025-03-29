import os
import time
import csv
import shutil
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.exceptions import ClickHouseError

from util import data_processing, data_pushing

UPLOAD_FOLDER = '/data/uploads'
EXTRACTED_FOLDER = '/data/uploads/extracted'
PROCESSED_FOLDER = '/data/processed'

PROCESSED_FILES = 'processed_files.txt'
TABLE_SCHEMA = 'table_schema.json'
RENAME_MAPPING = 'rename_mapping.json'


CLICKHOUSE_HOST="cd1.biron-analytics.com"
CLICKHOUSE_DATABASE="smallable2_playground"
CLICKHOUSE_USERNAME= os.getenv('CLICKHOUSE_USERNAME')
CLICKHOUSE_PASSWORD= os.getenv('CLICKHOUSE_PASSWORD')

client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8443, database=CLICKHOUSE_DATABASE, username=CLICKHOUSE_USERNAME, password=CLICKHOUSE_PASSWORD)

# Create folders if they don't exist
if not os.path.exists(PROCESSED_FILES):
    open(PROCESSED_FILES, 'w').close()

with open(PROCESSED_FILES, 'r') as f:
    processed_files = set(f.read().splitlines())

def main():
    """
    Process files in the UPLOAD_FOLDER and save processed files in table-specific subfolders
    within the PROCESSED_FOLDER before pushing to ClickHouse.
    Checks daily for new files, skips already processed ones, and handles deduplication
    both within and across files.
    """
    # Check for new files in the UPLOAD_FOLDER
    for filename in os.listdir(UPLOAD_FOLDER):
        file_path = os.path.join(UPLOAD_FOLDER, filename)

        # Skip if not a file or already processed or wrong format
        if not os.path.isfile(file_path) or filename in processed_files or not filename.endswith(('.csv', '.zip')):
            print(f"Skipping {filename}: Already processed, not a file, or invalid format.")
            continue

        print(f"\nProcessing file: {file_path}")

        # Extract if zip file
        if filename.endswith('.zip'):
            try:
                file_path = data_processing.extract_file(file_path, EXTRACTED_FOLDER)
                print(f"Extracted file: {file_path}")
            except Exception as e:
                print(f"Error extracting {file_path}: {e}")
                continue

        # Process only CSV files
        if not file_path or not file_path.endswith('.csv'):
            print(f"Skipping: {file_path} is not a valid CSV file after extraction")
            continue

        # Get base table name from filename
        raw_file_name = os.path.splitext(os.path.basename(file_path))[0]

        # Get table schema to determine table name
        table_schema = data_pushing.get_table_schema(table_name=raw_file_name, file_path=TABLE_SCHEMA)
        if not table_schema:
            print(f"No schema found for table {raw_file_name}, skipping...")
            continue

        # Use table name from schema or fallback to raw filename
        table_name = table_schema.get("table_name", raw_file_name)

        # Create table-specific subfolder in PROCESSED_FOLDER
        table_processed_folder = os.path.join(PROCESSED_FOLDER, table_name)
        os.makedirs(table_processed_folder, exist_ok=True)

        cleaned_file = os.path.join(table_processed_folder, f"{raw_file_name}_cleaned.csv")

        # Get column mapping for renaming
        column_names = data_processing.get_rename_columns(table_name=raw_file_name, file_path=RENAME_MAPPING)
        if not column_names:
            print(f"No column mapping found for {raw_file_name}, skipping...")
            continue

        # Rename columns
        print(f"\nRenaming columns in {file_path}...")
        data_processing.rename_column_in_csv(file_path, column_names, cleaned_file)

        # Check for missing columns
        print(f"\nChecking for missing columns in {cleaned_file}...")

        expected_columns = table_schema.get("column_names", [])

        # Read current columns
        encoding = data_processing.detect_encoding(cleaned_file)
        with open(cleaned_file, 'r', newline='', encoding=encoding) as csvfile:
            reader = csv.reader(csvfile)
            current_columns = next(reader)  # Get header row

        # Add missing columns
        for column_name in expected_columns:
            if column_name not in current_columns and column_name != 'id':
                position = expected_columns.index(column_name)
                print(f"Adding missing column {column_name} to {cleaned_file}...")
                temp_file = f"{os.path.splitext(cleaned_file)[0]}.temp.csv"
                data_processing.add_column_to_csv(cleaned_file, temp_file, column_name, position)
                shutil.move(temp_file, cleaned_file)

        # Remove duplicates within the file
        print(f"\nChecking for duplicates in {cleaned_file}...")
        data_processing.self_deduplicate_csv(cleaned_file, cleaned_file)

        # Mark file as processed
        with open(PROCESSED_FILES, "a") as f:
            f.write(f"{filename}\n")
        processed_files.add(filename)

        print('\nChecking for processed files...')

        # Get all existing files in the table-specific subfolder
        existing_files = [
            f for f in os.listdir(table_processed_folder)
            if os.path.isfile(os.path.join(table_processed_folder, f))
            and f != os.path.basename(cleaned_file)
        ]

        if len(existing_files) >= 1:  # At least 1 other file to compare with
            print("\nPerforming cross-file comparison...")
            for prev_file in existing_files:
                prev_file_path = os.path.join(table_processed_folder, prev_file)
                try:
                    print(f"\nComparing {cleaned_file} with {prev_file_path}...")
                    # Create a new dedup filename
                    dedup_file_path = os.path.join(table_processed_folder, f"dedup_{os.path.basename(cleaned_file)}")

                    data_processing.compare_and_deduplicate_csv_files(
                        prev_file_path,
                        cleaned_file,
                        dedup_file_path
                    )

                    # Then optionally replace the cleaned file if needed
                    shutil.move(dedup_file_path, cleaned_file)
                except Exception as e:
                    print(f"Error comparing {cleaned_file} and {prev_file_path}: {e}")
                    continue
        else:
            print("\nNo other files to compare with. Proceeding to insert.")

        # Insert into ClickHouse
        last_id = table_schema.get("last_id", 0)
        date_columns = table_schema.get("date_columns", [])
        int_columns = table_schema.get("int_columns", [])
        float_columns = table_schema.get("float_columns", [])
        string_columns = table_schema.get("string_columns", [])
        dob_columns = table_schema.get("dob_columns", [])

        print(f"Pushing {cleaned_file} to ClickHouse...")
        data_pushing.process_and_insert_csv(
            client,
            table_name,
            cleaned_file,
            chunk_size=10000,
            last_id=last_id,
            date_columns=date_columns,
            int_columns=int_columns,
            float_columns=float_columns,
            string_columns=string_columns,
            dob_columns=dob_columns
        )
        print(f"Successfully uploaded {cleaned_file} to ClickHouse")

        # Update last_id
        print(f"\nUpdating last_id for {table_name} ...")
        data_pushing.update_last_id(client, raw_file_name, TABLE_SCHEMA)



if __name__ == '__main__':
    while True:
        print("checking for new files...")
        main()
        time.sleep(30)
        # sleep for 24 hours
        # time.sleep(86400)