import zipfile
import csv
import hashlib
import sys
import time
import os
import shutil
import pandas as pd
from pathlib import Path
from itertools import islice

def get_rename_columns(table_name):
    print(f"Getting column names for table: {table_name}")
    column_names = None

    if 'smallable_campaign_events' in table_name:
        column_names = {
            "Email": "email",
            "prénom": "first_name",
            "nom": "last_name",
            "langue": "language",
            "Mobile": "mobile",
            "Campaign Event Type": "campaign_event_type",
            "Event Date": "event_date",
            "Event Datetime": "event_datetime",
            "Sending ID": "sending_id",
            "Campaign ID": "campaign_id",
            "Campaign Name": "campaign_name",
            "Campaign Category": "campaign_category",
            "Campaign Operation Code": "campaign_operation_code",
            "Email Subject": "email_subject",
            "Content id": "content_id",
            "Content Name": "content_name",
            "abonnements": "subscriptions",
            "Clicked Link": "clicked_link",
            "Unsubscribe Reason": "unsubscribe_reason",
            "IDENTIFIANT": "identifier",
            "CIVILITÉ": "civility",
            "ADRESSE_STREET1_FACTURATION": "billing_address_line_1",
            "ADRESSE_STREET2_FACTURATION": "billing_address_line_2",
            "CODEPOSTAL_FACTURATION": "billing_postal_code",
            "VILLE_FACTURATION": "billing_city",
            "PAYS_FACTURATION": "billing_country",
            "DATE DE NAISSANCE": "date_of_birth",
            "PARENTS": "parents",
            "NB_ENFANTS": "number_of_children",
            "ENFANT1_PRENOM": "child_1_first_name",
            "ENFANT1_SEXE": "child_1_gender",
            "ENFANT1_DATE_DE_NAISSANCE": "child_1_date_of_birth",
            "ENFANT2_PRENOM": "child_2_first_name",
            "ENFANT2_SEXE": "child_2_gender",
            "ENFANT2_DATE_DE_NAISSANCE": "child_2_date_of_birth",
            "ENFANT3_PRENOM": "child_3_first_name",
            "ENFANT3_SEXE": "child_3_gender",
            "ENFANT3_DATE_DE_NAISSANCE": "child_3_date_of_birth",
            "ENFANT4_PRENOM": "child_4_first_name",
            "ENFANT4_SEXE": "child_4_gender",
            "ENFANT4_DATE_DE_NAISSANCE": "child_4_date_of_birth",
            "ENFANT5_PRENOM": "child_5_first_name",
            "ENFANT5_SEXE": "child_5_gender",
            "ENFANT5_DATE_DE_NAISSANCE": "child_5_date_of_birth",
            "DATE_CREATION_COMPTE_SML": "SML_account_creation_date",
            "DATE_PREVU_ACCOUCHEMENT": "expected_delivery_date",
            "SOURCE_ENTREE_BASE_SML": "SML_source_entry",
            "LANGUE_CREATION_COMPTE": "account_creation_language",
            "OPTIN_NL": "NL_option",
            "DATE_INSCRIPTION_NL": "NL_registration_date",
            "NB_TOTAL_COMMANDES": "total_orders",
            "MONTANT_TOTAL_COMMANDES_EUR": "total_order_amount_eur",
            "DATE_1ERE_COMMANDE": "first_order_date",
            "DATE_DERNIERE_COMMANDE": "last_order_date",
            "SEGMENT_USERINTERVIEW": "segment_user_interview",
            "SEGMENT_RAMPUP": "segment_ramp_up",
            "SEGMENT_UR": "segment_ur",
            "CODE_OPE_FDP": "operation_code_fdp",
            "CODE_OPE_CHURN": "operation_code_churn",
            "SEGMENT_RFM": "segment_rfm",
            "SEGMENT_UR_THECLIENTISTHEBOSS": "segment_ur_theclientistheboss",
            "DEVISE_DERNIERE_COMMANDE": "currency_of_last_order",
            "CODE_SALARIE_NOEL": "employee_christmas_code",
            "PAYS_OPE_FID": "loyalty_operation_country",
            "OPE_FID_OCT22": "loyalty_operation_october_2022",
            "OPTIN_SME": "SME_option",
            "ADHESION_SME": "SME_membership",
            "OPE_FID_MARS23": "loyalty_operation_march_2023",
            "SHOPPING_PARTY_VIC": "shopping_party_vic",
            "DATE_ADHESION_SME": "SME_membership_date",
            "DATE_DESINSCRIPTION_SME": "SME_unsubscription_date",
            "NB_BONS_EMIS_AJD": "vouchers_issued_today",
            "DATE_BONS_EMIS_AJD": "date_vouchers_issued_today",
            "NB_BONS_DISPO": "available_vouchers",
            "NB_POINTS_DISPO": "available_points",
            "NB_TOTAL_BONS": "total_vouchers",
            "DATE_LAST_BON_EMIS": "last_voucher_issued_date",
            "DATE_EXPIRATION_BON": "voucher_expiration_date",
            "DATE_MAJ_POINT": "points_update_date",
            "Catégorie Bébé - Tous produits": "baby_category_all_products",
            "Catégorie Enfant-Ado - Tous produits": "child_teen_category_all_products",
            "Catégorie Femme - Mode, access & shoes": "women_category_fashion_accessories_shoes",
            "Catégorie Homme - Mode, access & shoes": "men_category_fashion_accessories_shoes",
            "Catégorie Maison - adulte - Autres Design": "home_category_adult_other_design",
            "Catégorie Maison - adulte - Mobilier": "home_category_adult_furniture",
            "Catégorie Soins et beauté adulte": "adult_care_beauty_category",
            "OPE_FID_MARS24": "loyalty_operation_march_2024",
            "DEVISE_OPE_FID_MARS24": "currency_loyalty_operation_march_2024",
            "Mobile International": "international_mobile",
            "Prénom Challenge Influence": "first_name_challenge_influence",
            "Montant et Devise Challenge Influence": "amount_currency_challenge_influence",
            "Code Challenge Influence": "challenge_influence_code",
            "Relance Challenge Influence": "challenge_influence_follow_up"
        }

    return column_names

def extract_file(file_path, output_path):
    """
    Extract the file in the given path
    :param file_path: path to the file
    :param output_path: path to the output directory
    :return: None
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_path)

    print(f"File extracted to {output_path}")
    return output_path + '/' + zip_ref.namelist()[0]


def rename_column_in_csv(file_path, column_name, output_file):
    """
    Rename a column in a CSV file
    :param file_path: path to the CSV file
    :param column_name: name of the column to rename
    :param output_file: path to save the output CSV file
    """

    # Remove the existing file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Old file {output_file} removed!")

    # Process each chunk
    for i, chunk in enumerate(pd.read_csv(file_path, 
                                          chunksize=100000, 
                                          dtype=str, 
                                          delimiter=";", 
                                          encoding="utf-8",
                                          )): 
        cleaned_chunk = chunk.rename(columns=column_name)

        # Save to file (Append after the first write)
        if i == 0:
            cleaned_chunk.to_csv(output_file, index=False, mode="w")  # Write with header
        else:
            cleaned_chunk.to_csv(output_file, index=False, mode="a", header=False)  # Append without header

        print(f"\rChunk {i+1} processed and saved!", end="", flush=True)

    print("\nColumne renamed completed and saved to:", output_file)

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

def compare_and_deduplicate_csv_files(comparative_file_path, target_file_path, output_file_path=None, chunk_size=100000, reuse_hashes=False):
    """
    Efficiently compare and deduplicate CSV files with minimal memory usage.
    Can reuse hashes from previous runs to avoid re-hashing the comparative file.

    Args:
    comparative_file_path (str): Path to the CSV to compare and deduplicate
    target_file_path (str): Path to the CSV to compare against
    output_file_path (str, optional): Path to save unique rows. If same as comparative_file_path, 
                                      will update the comparative file with unique rows.
    chunk_size (int, optional): Number of rows to process in memory at a time
    reuse_hashes (bool, optional): Whether to reuse previously generated hashes

    Returns:
    dict: A dictionary containing the row_hashes and headers of the comparative file
    """
    start_time = time.time()

    # Global dictionary to store hashes and headers
    global saved_row_hashes
    global saved_headers

    # Validate file paths
    file1 = Path(comparative_file_path)
    file2 = Path(target_file_path)

    if not file1.exists() or not file2.exists():
        print(f"Error: One or both files don't exist.")
        return 0

    # Print file information
    print(f"\nStarting comparison of files:")
    print(f"File 1: {file1.name} ({file1.stat().st_size / (1024 * 1024):.2f} MB)")
    print(f"File 2: {file2.name} ({file2.stat().st_size / (1024 * 1024):.2f} MB)")

    # Function to compare CSV structure
    def compare_csv_structure(file1_path, file2_path):
        with open(file1_path, 'r', newline='', encoding='utf-8') as f1:
            reader1 = csv.reader(f1)
            headers1 = next(reader1)
            
        with open(file2_path, 'r', newline='', encoding='utf-8') as f2:
            reader2 = csv.reader(f2)
            headers2 = next(reader2)
            
        columns_match = (headers1 == headers2)
        
        if not columns_match:
            print("Column structure mismatch:")
            print(f"File 1 headers: {headers1}")
            print(f"File 2 headers: {headers2}")
            
        return columns_match, headers1, headers2

    # First check column structure
    columns_match, headers1, headers2 = compare_csv_structure(comparative_file_path, target_file_path)

    if not columns_match:
        print("\nExiting due to column structure mismatch.")
        # TODO: Add option to proceed with column mismatch
        return 0

    # Check if we can reuse hashes
    if reuse_hashes and 'saved_row_hashes' in globals() and 'saved_headers' in globals() and saved_headers == headers1:
        print("Reusing existing hash set from previous run...")
        row_hashes = saved_row_hashes
        row_count_file1 = len(row_hashes) + 1  # +1 for header
    else:
        # Create a hash set from the comparative file
        print("Generating hash set from comparative file...")
        row_hashes = set()
        row_count_file1 = 0

        with open(comparative_file_path, 'r', newline='', encoding='utf-8') as f1:
            reader = csv.reader(f1)
            # Skip header
            headers1 = next(reader)
            row_count_file1 += 1

            # Process rows in chunks to limit memory usage
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
        
        # Save hashes globally for future use
        saved_row_hashes = row_hashes
        saved_headers = headers1

    # Now process the target file
    print("Processing target file for unique rows...")
    duplicate_count = 0
    unique_rows = 0
    row_count_file2 = 0

    # Prepare output file
    if output_file_path:
        # Create directory if it doesn't exist
        output_file_dir = Path(output_file_path).parent
        output_file_dir.mkdir(parents=True, exist_ok=True)

        # Check if output file is the same as comparative file
        if output_file_path == comparative_file_path:
            temp_output_path = f"{output_file_path}.temp"
            output_file = open(temp_output_path, 'w', newline='', encoding='utf-8')
        else:
            output_file = open(output_file_path, 'w', newline='', encoding='utf-8')
        
        writer = csv.writer(output_file)
    else:
        output_file = None
        writer = None

    with open(target_file_path, 'r', newline='', encoding='utf-8') as f2:
        reader = csv.reader(f2)
        # Read and write headers
        headers2 = next(reader)
        row_count_file2 += 1

        if output_file:
            writer.writerow(headers2)

        # Process rows in chunks
        for row in reader:
            row_hash = hashlib.md5(str(row).encode()).hexdigest()
            row_count_file2 += 1

            if row_hash not in row_hashes:
                if output_file:
                    writer.writerow(row)
                unique_rows += 1
                # Add to hash set for future comparisons
                row_hashes.add(row_hash)
            else:
                duplicate_count += 1

            # Print progress periodically
            if row_count_file2 % 100000 == 0:
                print(f"\rProcessed {row_count_file2:,} rows from target file...", end="", flush=True)

    # Close output file if opened
    if output_file:
        output_file.close()
        
        # If output is the same as comparative file, replace it
        if output_file_path == comparative_file_path:
            shutil.move(temp_output_path, output_file_path)
            print(f"\nUpdated {output_file_path} with unique rows.")
            
            # Update the global hash set with the new file
            saved_row_hashes = row_hashes

    # Calculate and print results
    end_time = time.time()
    duration = end_time - start_time

    print("\n--- Comparison Summary ---")
    print(f"Time taken: {duration:.2f} seconds")
    print(f"Total rows in comparative file: {row_count_file1:,}")
    print(f"Total rows in target file: {row_count_file2:,}")
    print(f"Number of duplicate rows found: {duplicate_count:,}")
    print(f"Number of unique rows: {unique_rows:,}")

    # Provide insights
    if duplicate_count == row_count_file2 - 1:  # Subtract 1 for header
        print("Files appear to be DUPLICATES (all rows match)")
    elif duplicate_count == 0:
        print("Files have NO duplicated rows")
    else:
        print(f"Files have {duplicate_count:,} duplicated rows")
        
    # Return the hash set for potential future use
    return {"row_hashes": row_hashes, "headers": headers1}

# TODO: add a function to find different columns between two files,
# add the missing columns to the second file with correct position and empty values

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