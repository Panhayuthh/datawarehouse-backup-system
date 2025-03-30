import pandas as pd
import json
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.exceptions import ClickHouseError

def handle_nan_for_type(df):
    """
    Handles NaN values in a DataFrame based on column data types:
    - Object (string) columns: Replace NaN with an empty string ('').
    - Float64 columns: Replace NaN with `pd.NA` (to maintain nullable float behavior).
    - Int64 columns: Replace NaN with `pd.NA` (as integers cannot have NaN).
    - DateTime columns: Replace NaN with `pd.NaT` (missing datetime value).

    Parameters:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The modified DataFrame with NaN values handled accordingly.
    """
    print("Handling NaN values...")

    # Handle object columns (replace NaN with empty string)
    df[df.select_dtypes(include='object').columns] = df[df.select_dtypes(include='object').columns].apply(lambda x: x.fillna(pd.NA).replace('<NA>', pd.NA))
    df[df.select_dtypes(include='object').columns] = df[df.select_dtypes(include='object').columns].apply(lambda x: x.fillna(pd.NA).replace('nan', pd.NA))

    # Handle float64 columns (replace NaN with 0.0)
    df[df.select_dtypes(include='Float64').columns] = df[df.select_dtypes(include='Float64').columns].fillna(pd.NA)

    # Handle int64 columns (replace NaN with 0)
    df[df.select_dtypes(include='Int64').columns] = df[df.select_dtypes(include='Int64').columns].fillna(pd.NA)

    # Handle DateTime columns (replace NaN with pd.NaT)
    df[df.select_dtypes(include='datetime64[ns]').columns] = df[df.select_dtypes(include='datetime64[ns]').columns].fillna(pd.NaT)

    return df

def find_problematic_rows(df, column, display_df=True, na=False):
    """
    Extracts and displays full rows where the specified column has issues.
    """
    if na == True:
        problem_rows = df[df[column].isna()]
    else:
      problem_rows = df[df[column].notna()]
    if not problem_rows.empty:
        print(f"\n🚨 Found {len(problem_rows)} problematic rows in column `{column}`")
        if display_df:
          print("Displaying full row data:\n")
          print(problem_rows.to_string(index=True))  # Print full row without index
    else:
        print(f"\nNo values found in `{column}`.")


def analyze_type_error(df, client, table):
    """
    Analyzes the DataFrame against the non-nullable columns of the ClickHouse table
    and identifies problematic rows.

    Parameters:
        df: DataFrame containing the data.
        client: ClickHouse client connection.
        table: Name of the ClickHouse table.
    """
    # Query the table schema to get non-nullable columns
    try:
        query = f"DESCRIBE TABLE {table}"
        result = client.query(query)

        # Extract non-nullable columns
        non_nullable_columns = []
        for row in result.result_rows:
            column_name = row[0]
            data_type = row[1]
            if "Nullable" not in data_type:
                non_nullable_columns.append(column_name)

        print(f"\n🔍 Non-nullable columns in table `{table}`: {non_nullable_columns}")

        # Check columns in the DataFrame against non-nullable columns
        for col in non_nullable_columns:
            if col in df.columns:
                dtype = df[col].dtype
                print(f"\n🔍 Investigating non-nullable column: {col} (dtype: {dtype})")
                find_problematic_rows(df, col, na=True)
            else:
                print(f"\n⚠️ Column `{col}` is non-nullable in the table but missing in the DataFrame.")

    except ClickHouseError as e:
        print(f"\n🚨 Error querying table schema for `{table}`: {e}")


def filter_duplicates(client, table, df, batch_size=1000):
    """
    Efficiently checks for duplicates using batched row_hash comparisons.
    """
    print(f"\nChecking for duplicates in {table}...")
    original_total_rows = len(df)
    
    # Early exit if the table is empty
    table_count = client.command(f"SELECT COUNT(*) FROM {table}")
    if table_count == 0:
        print(f"Table {table} is empty. Processing all {original_total_rows} rows.")
    
    # Extract unique hashes from the dataframe to reduce query size
    unique_hashes = df['row_hash'].unique().tolist()
    total_unique_hashes = len(unique_hashes)
    print(f"Checking {total_unique_hashes} unique hashes against database")
    
    # Find existing hashes in batches to manage memory and query size
    existing_hashes = set()
    for i in range(0, total_unique_hashes, batch_size):
        batch = unique_hashes[i:i+batch_size]
        formatted_hashes = "','".join(batch)
        query = f"SELECT DISTINCT row_hash FROM {table} WHERE row_hash IN ('{formatted_hashes}')"
        
        result = client.query_df(query)
        if not result.empty:
            existing_hashes.update(result['row_hash'].tolist())
        
        print(f"Processed batch {i//batch_size + 1}/{(total_unique_hashes + batch_size - 1)//batch_size}, "
              f"found {len(existing_hashes)} duplicates so far")
    
    # Filter out rows with existing hashes
    df_new = df[~df['row_hash'].isin(existing_hashes)]
    new_rows = len(df_new)
    existing_count = original_total_rows - new_rows
    
    print(f"\nDuplicate check results:")
    print(f"- Total rows in chunk: {original_total_rows}")
    print(f"- Existing rows found: {existing_count}")
    print(f"- New rows to insert: {new_rows}")
    
    return df_new

def get_table_schema(table_name, file_path):
    """
    Load schema from JSON file

    Parameters:
        table_name: The name of the table (string).
        file_path: The path to the JSON file containing the schema.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)

    print(f"\nLoading schema for table: {table_name}")
    for table in schema:
        if table in table_name:
            return schema[table]

    return {}  # Return empty dict if table not found


def insert_new_data(client, table, df, chunk_size=1000):
    """
    Insert new data into ClickHouse table after checking for duplicates.
    Returns a tuple: (success_status, number_of_rows_inserted)
    Skips insertion if all data exists and continues processing.

    Parameters:
        client: A ClickHouse client instance.
        table: The name of the target ClickHouse table (string).
        df: A Pandas DataFrame containing the data to be inserted. Must include an 'id' column.
        chunk_size: Number of rows per chunk (default: 1000).

    Returns:
        tuple: (bool, int) - (success status, number of new rows inserted)
    """
    if df.empty:
        print("No data available in this chunk.")
        return True, 0

    print(f"\nInsertion plan:")
    print(f"- Chunk size: {chunk_size}")
    print(f"- Total rows to insert: {len(df)}")

    try:

        client.insert_df(
            table=table,
            df=df,
        )

        print(f"\nSuccessfully inserted {len(df)} rows into {table}!")
        return True, len(df)

    except DataError as e:
        print("\nDataError encountered while inserting!")
        error_message = str(e)
        print(error_message)
        if "for source column " in error_message:
            try:
                col_name = error_message.split("for source column ")[1].split("")[0]
            except Exception:
                col_name = "Unknown"
            print(f"\n🔍 Investigating problematic column: {col_name}`")
            find_problematic_rows(df, col_name)
        return False, 0

    except TypeError as e:
        print("\nTypeError encountered while inserting!")
        error_message = str(e)
        print(error_message)
        analyze_type_error(df, client, table)
        return False, 0

    except AttributeError as e:
        print("\nAttributeError encountered while inserting!")
        error_message = str(e)
        print(error_message)
        return False, 0
    
    except Exception as e:
        print("\nGeneral error encountered while inserting!")
        error_message = str(e)
        print(error_message)
        return False, 0

def process_and_insert_csv(client, table, csv_path, chunk_size=10000, last_id=None,
                         date_columns=None, int_columns=None, float_columns=None,
                         string_columns=None, dob_columns=None, percentage_columns=None):
    """
    Reads a CSV file in chunks, processes each chunk, and inserts data into ClickHouse.
    Automatically handles encoding issues and ensures data integrity.

    Parameters:
        client: ClickHouse client instance.
        table: Target ClickHouse table name.
        csv_path: Path to the CSV file.
        chunk_size: Number of rows per chunk (default 10,000).
        last_id: If None, auto-detects the last ID in the table.
        date_columns: List of date columns to convert.
        int_columns: List of integer columns to convert.
        float_columns: List of float columns to convert.
        string_columns: List of string columns to convert.
        dob_columns: List of date-of-birth columns to convert.
        percentage_columns: List of percentage columns to normalize (e.g., "50%" → 0.5).
    """
    print("\n🚀 Starting CSV processing in chunks...")

    # Try UTF-8 first, fall back to latin1 if needed
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            total_rows = sum(1 for _ in f) - 1  # Subtract header
    except UnicodeDecodeError:
        with open(csv_path, 'r', encoding='latin1') as f:
            total_rows = sum(1 for _ in f) - 1

    total_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size else 0)
    print(f"📊 Total rows: {total_rows} | Chunks: {total_chunks}")

    # Initialize stats
    total_rows_inserted = 0
    chunks_processed = 0
    chunks_skipped = 0

    # Auto-detect last_id if not provided
    if last_id is None:
        last_id = 0
    print(f"🆔 Starting IDs from {last_id + 1}")

    # Read CSV with error handling
    try:
        chunk_iter = pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            encoding='utf-8',
        )
    except UnicodeDecodeError:
        chunk_iter = pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            encoding='latin1',
        )

    for current_chunk, df_chunk in enumerate(chunk_iter, 1):
        print(f"\n📌 Processing chunk {current_chunk}/{total_chunks} ({len(df_chunk)} rows)")

        # Skip duplicates
        df_new = filter_duplicates(client, table, df_chunk)
        if len(df_new) == 0:
            print(f"⏭️ All rows exist. Skipping chunk {current_chunk}.")
            chunks_skipped += 1
            continue

        # Assign incremental IDs
        df_new.insert(0, 'id', range(last_id + 1, last_id + 1 + len(df_new)))
        last_id += len(df_new)

        print("Converting data types...")
        df_new = df_new.astype({col: 'Float64' for col in df_new.select_dtypes(include='float64').columns})

        # Convert date columns
        for col in date_columns or []:
            if col in df_new.columns:
                df_new[col] = pd.to_datetime(df_new[col], errors="coerce")

        # Convert int columns
        for col in int_columns or []:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors="coerce").astype('Int64')

        # Convert float columns
        for col in float_columns or []:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors='coerce').astype('Float64')

        df_new = df_new.astype({col: 'Float64' for col in df_new.select_dtypes(include='float64').columns})

        # Convert string columns
        for col in string_columns or []:
            if col in df_new.columns:
                df_new[col] = df_new[col].astype(str).str.replace(r'\.0$', '', regex=True)

        # Convert date of birth columns
        for col in dob_columns or []:
            if col in df_new.columns:
                df_new[col] = df_new[col].dt.strftime('%Y-%m-%d')

        df_new = handle_nan_for_type(df_new)

        # Insert into ClickHouse
        success, rows_inserted = insert_new_data(client, table, df_new)
        if success:
            total_rows_inserted += rows_inserted
        else:
            return {
                "success": False,
                "error": f"Failed to insert chunk {current_chunk}"
            }

        chunks_processed += 1

    # Final report
    print("\n📊 Results:")
    print(f"- Chunks processed: {chunks_processed}/{total_chunks}")
    print(f"- Chunks skipped (duplicates): {chunks_skipped}")
    print(f"- Total rows inserted: {total_rows_inserted}")
    print(f"- Last used ID: {last_id}")

    return {
        "success": True,
        "chunks_processed": chunks_processed,
        "chunks_skipped": chunks_skipped,
        "total_rows_inserted": total_rows_inserted,
        "last_id": last_id
    }


def get_last_id(client, clickhouse_table):
    """
    Get the last ID from the ClickHouse table.

    Parameters:
        client: ClickHouse client instance.
        clickhouse_table: Target ClickHouse table name.

    Returns:
        int: The last ID in the table.
    """
    try:
        query = f"SELECT MAX(id) FROM {clickhouse_table}"
        result = client.query(query)
        last_id = result.first_row[0]
        print(f"last_id: {last_id}")
        return last_id
    except ClickHouseError as e:
        print(f"Error getting last ID for {clickhouse_table}: {e}")
        return 0

def update_last_id(client, table_name, file_path):
    """
    Update the last_id in the table_schema.json file.

    Parameters:
        table_name: Name of the table.
        file_path: Path to the CSV file.
    """

    with open(file_path, 'r') as f:
        table_schema = json.load(f)

    for table in table_schema:
      if table in table_name:
        clickhouse_table = table_schema[table]["table_name"]

    last_id = get_last_id(client, clickhouse_table)
    try:
        table_schema[table]["last_id"] = last_id
    except KeyError:
        print(f"Table {table} not found in schema. Adding it.")
        return {"success": False, "error": f"Table {table} not found in schema."}

    with open(file_path, 'w') as f:
        json.dump(table_schema, f, indent=4)

    print(f"last_id updated for {table}")
    
    return {"success": True, "last_id": last_id}