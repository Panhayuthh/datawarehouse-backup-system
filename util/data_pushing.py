import csv
import pandas as pd
import hashlib
import sys
import time
from pathlib import Path
from itertools import islice
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.exceptions import ClickHouseError


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

def filter_duplicates(client, table, df):
    """
    Checks for existing rows in ClickHouse for the given DataFrame chunk
    (based on the 'id' column) and filters out duplicate rows.

    Parameters:
        client: A ClickHouse client instance.
        table: The target ClickHouse table name (string).
        df: A Pandas DataFrame chunk containing an 'id' column.

    Returns:
        tuple: (df_new, original_total_rows, existing_count)
            df_new: DataFrame after filtering out rows with existing IDs.
            original_total_rows: The number of rows in the original DataFrame chunk.
            existing_count: The number of rows that were duplicates.
    """
    original_total_rows = len(df)

    # Check table row count
    table_count = client.command(f"SELECT COUNT(*) FROM {table}")

    if table_count == 0:
        existing_ids = set()
        print(f"Table {table} is empty. Processing {original_total_rows} rows.")
    else:
        # Get existing IDs only for the current chunk's ID range
        min_id = df['id'].min()
        max_id = df['id'].max()
        existing_ids_query = f"SELECT id FROM {table} WHERE id >= {min_id} AND id <= {max_id}"
        existing_ids_df = client.query_df(existing_ids_query)
        existing_ids = set(existing_ids_df['id'].tolist()) if not existing_ids_df.empty else set()
        print(f"Found {len(existing_ids)} existing IDs in {table}")

    # Filter out duplicates
    df_new = df[~df['id'].isin(existing_ids)]
    new_rows = len(df_new)
    existing_count = original_total_rows - new_rows

    print(f"\nDuplicate check results:")
    print(f"- Total rows in chunk: {original_total_rows}")
    print(f"- Existing rows found: {existing_count}")
    print(f"- New rows to insert: {new_rows}")

    return df_new, original_total_rows, existing_count

def get_column_names(table):
    """
    Get the column names of a ClickHouse table
    :param table: name of the table
    :return: list of column names
    """

    if table == "splio_export_tmp":
        column_names = ['id', 'email', 'first_name', 'last_name', 'language', 'mobile', 'campaign_event_type',
                    'event_date', 'event_datetime', 'sending_id', 'campaign_id', 'campaign_name', 'campaign_category',
                    'campaign_operation_code', 'email_subject', 'content_id', 'content_name', 'subscriptions', 'clicked_link',
                    'unsubscribe_reason', 'identifier', 'civility', 'billing_address_line_1', 'billing_address_line_2', 'billing_postal_code',
                    'billing_city', 'billing_country', 'date_of_birth', 'parents', 'number_of_children', 'child_1_first_name', 'child_1_gender',
                    'child_1_date_of_birth', 'child_2_first_name', 'child_2_gender', 'child_2_date_of_birth', 'child_3_first_name', 'child_3_gender',
                    'child_3_date_of_birth', 'child_4_first_name', 'child_4_gender', 'child_4_date_of_birth', 'child_5_first_name', 'child_5_gender',
                    'child_5_date_of_birth', 'SML_account_creation_date', 'expected_delivery_date', 'SML_source_entry', 'account_creation_language',
                    'NL_option', 'NL_registration_date', 'total_orders', 'total_order_amount_eur', 'first_order_date', 'last_order_date',
                    'segment_user_interview', 'segment_ramp_up', 'segment_ur', 'operation_code_fdp', 'operation_code_churn', 'segment_rfm',
                    'segment_ur_theclientistheboss', 'currency_of_last_order', 'employee_christmas_code', 'loyalty_operation_country',
                    'loyalty_operation_october_2022', 'SME_option', 'SME_membership', 'loyalty_operation_march_2023', 'shopping_party_vic',
                    'SME_membership_date', 'SME_unsubscription_date', 'vouchers_issued_today', 'date_vouchers_issued_today', 'available_vouchers',
                    'available_points', 'total_vouchers', 'last_voucher_issued_date', 'voucher_expiration_date', 'points_update_date',
                    'baby_category_all_products', 'child_teen_category_all_products', 'women_category_fashion_accessories_shoes',
                    'men_category_fashion_accessories_shoes', 'home_category_adult_other_design', 'home_category_adult_furniture',
                    'adult_care_beauty_category', 'loyalty_operation_march_2024', 'currency_loyalty_operation_march_2024', 'international_mobile',
                    'first_name_challenge_influence', 'amount_currency_challenge_influence', 'challenge_influence_code', 'challenge_influence_follow_up']
    
    return column_names

def get_column_type_names(table):
    """
    Get the column data types of a ClickHouse table
    :param table: name of the table
    """

    if table == "splio_export_tmp":
        column_type_names = ['UInt64', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(DateTime)', 'Nullable(DateTime)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(Int64)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(DateTime)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(DateTime)', 'Nullable(Int64)', 'Nullable(Float64)', 'Nullable(DateTime)', 'Nullable(DateTime)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(Float64)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(Float64)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(DateTime)', 'Nullable(DateTime)', 'Nullable(Int64)',
                                'Nullable(DateTime)', 'Nullable(Int64)', 'Nullable(Int64)', 'Nullable(Int64)', 'Nullable(DateTime)', 'Nullable(DateTime)',
                                'Nullable(DateTime)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)', 'Nullable(String)',
                                'Nullable(String)', 'Nullable(String)', 'Nullable(String)']
        
    return column_type_names


def insert_new_data(client, table, df, chunk_size=1000):
    """
    Insert new data into ClickHouse table after checking for duplicates.
    Now returns a tuple: (success_status, number_of_rows_inserted)
    Skips insertion if all data exists and continues processing.

    Parameters:
        client: A ClickHouse client instance.
        table: The name of the target ClickHouse table (string).
        df: A Pandas DataFrame containing the data to be inserted. Must include an 'id' column.
        chunk_size: Number of rows per chunk.

    Returns:
        tuple: (bool, int) - (success status, number of new rows inserted)
    """
    if df.empty:
        print("No data available in this chunk.")
        return True, 0

    column_names = get_column_names(table)
    column_type_names = get_column_type_names(table)

    # Create chunks from the filtered DataFrame
    total_rows = len(df)
    num_chunks = total_rows // chunk_size + (1 if total_rows % chunk_size != 0 else 0)
    chunks = [df.iloc[i * chunk_size: (i + 1) * chunk_size] for i in range(num_chunks)]

    print(f"\nInsertion plan:")
    print(f"- Chunk size: {chunk_size}")
    print(f"- Total new chunks needed: {num_chunks}")

    # Insert the chunks into ClickHouse
    if chunks:
        first_chunk = chunks[0]
        try:
            client.insert_df(table, first_chunk, column_names=column_names, column_type_names=column_type_names)
            print(f"\n✅ Successfully inserted the first chunk into {table}!")

            return True, total_rows
        
        except DataError as e:
            print("\n🚨 DataError encountered while inserting the first chunk!")
            error_message = str(e)
            print(error_message)
            if "for source column `" in error_message:
                try:
                    col_name = error_message.split("for source column `")[1].split("`")[0]
                except Exception:
                    col_name = "Unknown"
                print(f"\n🔍 Investigating problematic column: `{col_name}`")
                find_problematic_rows(df, col_name)
            return False, 0
        
        except TypeError as e:
            print("\n🚨 TypeError encountered while inserting the first chunk!")
            error_message = str(e)
            print(error_message)
            analyze_type_error(df, client, table)
            return False, 0
        
        except AttributeError as e:
            print("\n🚨 AttributeError encountered while inserting the first chunk!")
            error_message = str(e)
            print(error_message)
            return False, 0
