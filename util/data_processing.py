import zipfile
import csv
import hashlib
import sys
import time
import os
import re
import pandas as pd
from pathlib import Path
from itertools import islice
import chardet
from collections import Counter
import json
from contextlib import ExitStack

def get_rename_columns(table_name, file_path):
    """
    Get the rename mapping for columns from a JSON file.

    Parameters
        table_name: Name of the table
        file_path: Path to the JSON file containing rename mappings
    returns
        Dictionary of rename mappings for the specified table
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        rename_mappings = json.load(file)

    # Look for partial matches instead of exact matches
    for mapped_table in rename_mappings:
        if mapped_table in table_name:
            return rename_mappings[mapped_table]

    return {}

def extract_file(file_path, output_path):
    """
    Extract the file in the given path and rename it.

    :param file_path: path to the zip file
    :param output_path: path to the output directory
    :return: Path to the renamed extracted file
    """
    os.makedirs(output_path, exist_ok=True)  # Ensure output directory exists
    print(f"Extracting {file_path} to {output_path}...")

    # Extract the file
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_path)
        extracted_file_name = zip_ref.namelist()[0]  # Get the name of the first file in the zip

    # Construct the full path of the extracted file
    extracted_file_path = output_path + '/' + extracted_file_name

    # Extract the base name
    base_name = extracted_file_name.split('/')[-1].rsplit('_', 1)[0]

    # Extract the date from the original zip file name
    match = re.search(r'\d+', os.path.basename(file_path))
    if match:
        file_date = match.group()  # Use the numeric part from the zip file name
    else:
        file_date = time.strftime("%Y%m%d")  # Use current date if no numeric part is found

    # Construct the new file name
    new_file_name = f"{base_name}_{file_date}.csv"
    new_file_path = output_path + '/' + new_file_name

    # Rename the extracted file
    os.rename(extracted_file_path, new_file_path)

    print(f"File extracted and renamed to {new_file_path}")
    return new_file_path

def detect_delimiter_manual(file_path):
    with open(file_path, 'r') as file:
        first_line = file.readline()
        delimiters = [',', ';', '\t', '|']
        counts = Counter({delim: first_line.count(delim) for delim in delimiters})
        return counts.most_common(1)[0][0]

def detect_encoding(file_path, sample_size=100000):
    """
    Detects the encoding of a given file by analyzing a sample of bytes.

    :param file_path: Path to the file to analyze.
    :param sample_size: Number of bytes to read for encoding detection (default: 100KB).
    :return: Detected encoding.
    """
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read(sample_size))
    return result["encoding"]

def rename_column_in_csv(file_path, column_mapping, output_file):
    """
    Rename a column in a CSV file while handling different encodings.

    :param file_path: Path to the CSV file.
    :param column_mapping: Dictionary with {old_column_name: new_column_name}.
    :param output_file: Path to save the output CSV file.
    """

    # Detect encoding
    encoding = detect_encoding(file_path)
    print(f"Detected encoding: {encoding}")

    # Remove the existing output file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Old file {output_file} removed!")

    # Process the file in chunks
    for i, chunk in enumerate(pd.read_csv(file_path,
                                          chunksize=100000,
                                          dtype=str,
                                          delimiter=";",
                                          encoding=encoding,
                                          )):
        cleaned_chunk = chunk.rename(columns=column_mapping)

        # Save to file (Append after the first write)
        if i == 0:
            cleaned_chunk.to_csv(output_file, index=False, mode="w")  # Write with header
        else:
            cleaned_chunk.to_csv(output_file, index=False, mode="a", header=False)  # Append without header

        print(f"\rChunk {i+1} processed and saved!", end="", flush=True)

    print("\nColumn rename completed and saved to:", output_file)

def get_row_hash(row):
    """Create a hash of the row to efficiently compare rows"""
    return hashlib.md5(str(row).encode()).hexdigest()

def compare_csv_structure(file1_path, file2_path):
    """
    Compare the column structure of two CSV files
    Returns a tuple (columns_match, headers1, headers2)
    """
    headers1 = []
    headers2 = []

    # Read headers from first file
    with open(file1_path, 'r', newline='') as f1:
        reader = csv.reader(f1)
        headers1 = next(reader)

    # Read headers from second file
    with open(file2_path, 'r', newline='') as f2:
        reader = csv.reader(f2)
        headers2 = next(reader)

    # Compare column count
    if len(headers1) != len(headers2):
        print("\nMISMATCH: Files have different number of columns")
        return False, headers1, headers2

    # Compare column names
    column_matches = all(h1 == h2 for h1, h2 in zip(headers1, headers2))
    if not column_matches:
        print("\nMISMATCH: Column names differ between files")
        # Count of mismatched columns
        mismatch_count = sum(1 for h1, h2 in zip(headers1, headers2) if h1 != h2)
        print(f"  {mismatch_count} columns have different names")
    else:
        print("\nMATCH: Both files have identical column structure")

    return column_matches, headers1, headers2

def compare_and_deduplicate_csv_files(comparative_file_path, target_file_path, output_file_path=None, chunk_size=100000):
    """
    Efficiently compare and deduplicate CSV files with minimal memory usage.
    Now prevents output_file_path from being the same as comparative_file_path for safety.

    Args:
        comparative_file_path (str): Path to the CSV to compare against (will NOT be modified)
        target_file_path (str): Path to the CSV to deduplicate
        output_file_path (str, optional): Path to save unique rows (cannot be same as comparative_file_path)
        chunk_size (int, optional): Number of rows to process in memory at a time
    """
    start_time = time.time()

    # Validate output path safety
    if output_file_path:
        output_file_path = Path(output_file_path).resolve()
        comparative_path = Path(comparative_file_path).resolve()

        if output_file_path == comparative_path:
            raise ValueError("Output path cannot be the same as the comparative file path")

    # set max field size limit to maximum
    try:
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        csv.field_size_limit(2147483647)  # 2^31 - 1

    # Validate file paths
    file1 = Path(comparative_file_path)
    file2 = Path(target_file_path)

    if not file1.exists():
        raise FileNotFoundError(f"Comparative file not found: {comparative_file_path}")
    if not file2.exists():
        raise FileNotFoundError(f"Target file not found: {target_file_path}")

    # Print file information
    print(f"Starting comparison of files:")
    print(f"Comparative File: {file1.name} ({file1.stat().st_size / (1024 * 1024):.2f} MB)")
    print(f"Target File: {file2.name} ({file2.stat().st_size / (1024 * 1024):.2f} MB)")

    # First check column structure
    columns_match, headers1, headers2 = compare_csv_structure(comparative_file_path, target_file_path)
    if not columns_match:
        print("\nExiting program due to column structure mismatch.")
        return 0

    # Generate hash set from comparative file
    print("Generating hash set from comparative file...")
    row_hashes = set()
    row_count_file1 = 0

    with open(comparative_file_path, 'r', newline='', encoding='utf-8') as f1:
        reader = csv.reader(f1)
        headers1 = next(reader)
        row_count_file1 += 1

        while True:
            chunk = list(islice(reader, chunk_size))
            if not chunk:
                break

            for row in chunk:
                row_hash = hashlib.md5(str(row).encode()).hexdigest()
                row_hashes.add(row_hash)
                row_count_file1 += 1

            print(f"\rProcessed {row_count_file1:,} rows from comparative file...", end="", flush=True)

    print(f"\nCompleted processing {row_count_file1:,} rows from comparative file.")

    # Process target file
    print("Processing target file for unique rows...")
    duplicate_count = 0
    unique_rows = 0
    row_count_file2 = 0

    # Prepare output handling
    if output_file_path:
        output_dir = Path(output_file_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        output_mode = 'w'
    else:
        output_mode = None

    with ExitStack() as stack:
        # Open output file if specified
        if output_mode:
            output_file = stack.enter_context(open(output_file_path, output_mode, newline='', encoding='utf-8'))
            writer = csv.writer(output_file)
            writer.writerow(headers2)
        else:
            writer = None

        with open(target_file_path, 'r', newline='', encoding='utf-8') as f2:
            reader = csv.reader(f2)
            headers2 = next(reader)
            row_count_file2 += 1

            for row in reader:
                row_hash = hashlib.md5(str(row).encode()).hexdigest()
                row_count_file2 += 1

                if row_hash not in row_hashes:
                    if writer:
                        writer.writerow(row)
                    unique_rows += 1
                else:
                    duplicate_count += 1

                if row_count_file2 % 100000 == 0:
                    print(f"\rProcessed {row_count_file2:,} rows from target file...", end="", flush=True)

    # Results summary
    end_time = time.time()
    duration = end_time - start_time

    print("\n--- Comparison Summary ---")
    print(f"Time taken: {duration:.2f} seconds")
    print(f"Total rows in comparative file: {row_count_file1:,}")
    print(f"Total rows in target file: {row_count_file2:,}")
    print(f"Duplicate rows found: {duplicate_count:,}")
    print(f"Unique rows kept: {unique_rows:,}")

    if output_file_path:
        print(f"\nDeduplicated output saved to: {output_file_path}")


def self_deduplicate_csv(input_file_path, output_file_path=None, chunk_size=100000):
    """
    Efficiently remove duplicate rows within a single CSV file with minimal memory usage.

    Args:
    input_file_path (str): Path to the CSV file to deduplicate
    output_file_path (str, optional): Path to save deduplicated rows. If None, will create a new file with "_deduplicated" suffix.
    chunk_size (int, optional): Number of rows to process in memory at a time
    preserve_order (bool, optional): Whether to preserve the original order of rows (first occurrence kept)

    Returns:
    dict: A dictionary containing statistics about the deduplication process
    """

    start_time = time.time()

    # Validate file path
    file_path = Path(input_file_path)

    if not file_path.exists():
        print(f"Error: File doesn't exist: {input_file_path}")
        return {"success": False, "error": "File not found"}

    # Set default output path if not provided
    if not output_file_path:
        output_file_path = str(file_path.with_name(f"{file_path.stem}_deduplicated{file_path.suffix}"))

    # Print file information
    print(f"Starting self-deduplication:")
    print(f"File: {file_path.name} ({file_path.stat().st_size / (1024 * 1024):.2f} MB)")
    print(f"Output will be saved to: {output_file_path}")

    # Create directory for output file if it doesn't exist
    output_path = Path(output_file_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use a temporary file if the output is the same as the input
    if output_file_path == input_file_path:
        temp_output_path = f"{output_file_path}.temp"
        actual_output_path = temp_output_path
    else:
        actual_output_path = output_file_path

    # Track duplicates and statistics
    row_hashes = set()
    row_count = 0
    unique_count = 0
    duplicate_count = 0

    # set max field size limit to maximum
    try:
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        # Use a smaller value
        csv.field_size_limit(2147483647)  # 2^31 - 1

    # Process the file
    with open(input_file_path, 'r', newline='', encoding='utf-8') as input_file, \
         open(actual_output_path, 'w', newline='', encoding='utf-8') as output_file:

        reader = csv.reader(input_file)
        writer = csv.writer(output_file)

        # Read and write headers
        try:
            headers = next(reader)
            writer.writerow(headers)
            row_count += 1
            unique_count += 1
        except StopIteration:
            print("Warning: Empty input file or no headers found.")
            return {"success": False, "error": "Empty file"}

        # Process remaining rows
        for row in reader:
            row_count += 1

            # Create hash for the row
            row_hash = hashlib.md5(str(row).encode()).hexdigest()

            # Check if this is a duplicate
            if row_hash not in row_hashes:
                # Write to output file
                writer.writerow(row)
                row_hashes.add(row_hash)
                unique_count += 1
            else:
                duplicate_count += 1

            # Print progress periodically
            if row_count % chunk_size == 0:
                print(f"\rProcessed {row_count:,} rows, found {duplicate_count:,} duplicates...", end="", flush=True)

    # If output is the same as input, replace the original file
    if output_file_path == input_file_path:
        import shutil
        shutil.move(temp_output_path, output_file_path)
        print(f"\nUpdated original file with deduplicated rows.")

    # Calculate and print results
    end_time = time.time()
    duration = end_time - start_time

    print("\n--- Deduplication Summary ---")
    print(f"Time taken: {duration:.2f} seconds")
    print(f"Total rows processed: {row_count:,}")
    print(f"Duplicate rows removed: {duplicate_count:,}")
    print(f"Unique rows remaining: {unique_count:,}")
    print(f"Deduplicated file saved to: {output_file_path}")

    if duplicate_count == 0:
        print("No duplicates found - file already contains unique rows only")
    else:
        print(f"Removed {duplicate_count:,} duplicate rows ({(duplicate_count / row_count) * 100:.2f}% of total)")

def add_column_to_csv(input_file, output_file, column_name, position):
    """
    Adds an empty column to a large CSV file at a specified position without loading the entire file into memory.

    Parameters:
        input_file (str): Path to the original large CSV file.
        output_file (str): Path to save the modified CSV file.
        column_name (str): Name of the new column.
        position (int): Index position to insert the column (0-based).
    """
    start_time = time.time()
    print(f"Adding column '{column_name}' at position {position} to '{input_file}'...")

    if os.path.exists(output_file):
      os.remove(output_file)
      print(f"Old file {output_file} removed!")

    # Open the input and output CSV files
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Process header (first row)
        header = next(reader)
        header.insert(position, column_name)  # Insert the new column at the given position
        writer.writerow(header)

        # Process each data row and insert an empty column
        for row in reader:
            row.insert(position, "")  # Insert an empty value
            writer.writerow(row)

    end_time = time.time()
    duration = end_time - start_time
    print(f"Column added in {duration:.2f} seconds.")
    print(f"Column '{column_name}' added at position {position} in '{output_file}'.")