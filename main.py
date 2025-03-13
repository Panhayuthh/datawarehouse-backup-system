import os
import time
import util.data_processing as processing

UPLOAD_FOLDER = 'data/uploads'
EXTRACTED_FOLDER = 'data/uploads/extracted'
PROCESSED_FOLDER = 'data/processed'

PROCESSED_FILES = 'processed_files.txt'
# TODO: add s3 bucket support

if not os.path.exists(PROCESSED_FILES):
    open(PROCESSED_FILES, 'w').close()

with open(PROCESSED_FILES, 'r') as f:
    processed_files = set(f.read().splitlines())

def main():
    """
    main function to process files in the UPLOAD_FOLDER, and save the processed files in the PROCESSED_FOLDER before pushing to clickhouse
    the function will check everyday if there are new files in the UPLOAD_FOLDER and process them
    before processing, the function check if the file is already processed and skip it
    """

    # check for new files in the UPLOAD_FOLDER
    for file in os.listdir(UPLOAD_FOLDER):
        if os.path.isfile:
            file_path = UPLOAD_FOLDER + '/' + file

            # Check for correct file format
            if file.endswith(('.csv', '.zip')) and file not in processed_files:
                print(f"Processing file: {file_path}")

                # If file is zip, extract it
                if file.endswith('.zip'):
                    file_path = processing.extract_file(file_path, EXTRACTED_FOLDER)
                    print(f"Extracted file: {file_path}")

                if file_path and file_path.endswith('.csv'):
                    renamed_file = PROCESSED_FOLDER + '/' + file + '_renamed.csv'

                    column_names = processing.get_rename_columns(table_name=file_path.split('/')[-1].split('.')[0])
                    if column_names:
                        processing.rename_column_in_csv(file_path, column_names, renamed_file)
                        saved_row_hashes = set()
                        saved_header = []

                        for file in os.listdir(PROCESSED_FOLDER):
                            file_path = PROCESSED_FOLDER + '/' + file
                            if os.path.isfile(file_path) and file_path != renamed_file:
                                processing.compare_and_deduplicate_csv_files(renamed_file, file_path, renamed_file, reuse_hashes=True)

                    # Mark the file as processed
                    with open(PROCESSED_FILES, "a") as f:
                        f.write(file + "\n")
                    processed_files.add(file)

            else:
                print(f"Skipping {file}: Already processed or invalid format.")




if __name__ == '__main__':
    while True:
        print("checking for new files...")
        main()
        time.sleep(30)
        # sleep for 24 hours
        # time.sleep(86400)