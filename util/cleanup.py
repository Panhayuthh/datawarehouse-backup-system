import os
import logging
import time
import datetime

logger = logging.getLogger(__name__)

UPLOAD_FOLDER = 'data/uploads/'
EXTRACTED_FOLDER = 'data/uploads/extracted'
PROCESSED_FOLDER = 'data/processed'

MAX_STORAGE_SIZE = 20 * 1024 * 1024 * 1024
MAX_FOLDER_AGE_DAYS = 7 # Default maximum age for files to be cleaned up

def cleanup_files(*file_paths):
    """
    Clean up one or more files if they exist. Logs errors without failing.
    
    Args:
        *file_paths: Variable number of file paths to remove
    """
    for path in file_paths:
        if path and os.path.exists(path):
            try:
                os.remove(path)
                logger.info(f"Removed file: {path}")
            except Exception as e:
                logger.warning(f"Failed to remove file {path}: {e}")

def get_folder_size(folder_path):
    """
    Calculate the total size of a folder in bytes.
    
    Args:
        folder_path: Path to the folder
        
    Returns:
        int: Total size in bytes
    """
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                total_size += os.path.getsize(file_path)
    return total_size

def cleanup_old_files(base_folder, max_age_days=MAX_FOLDER_AGE_DAYS):
    """
    Remove files older than max_age_days from the specified folder and its subfolders.
    
    Args:
        base_folder: Base folder to clean up
        max_age_days: Maximum age of files in days
    """
    current_time = time.time()
    cutoff_time = current_time - (max_age_days * 24 * 60 * 60)
    
    removed_count = 0
    removed_size = 0
    
    for dirpath, dirnames, filenames in os.walk(base_folder):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                file_mtime = os.path.getmtime(file_path)
                if file_mtime < cutoff_time:
                    file_size = os.path.getsize(file_path)
                    try:
                        os.remove(file_path)
                        removed_count += 1
                        removed_size += file_size
                        logger.info(f"Removed old file: {file_path} (last modified: {datetime.datetime.fromtimestamp(file_mtime)})")
                    except Exception as e:
                        logger.warning(f"Failed to remove old file {file_path}: {e}")
    
    if removed_count > 0:
        logger.info(f"Cleanup complete: Removed {removed_count} files ({round(removed_size / (1024 * 1024), 2)} MB)")
    return removed_count, removed_size

def check_storage_and_cleanup():
    """
    Check storage usage and clean up if necessary.
    Removes oldest files first until below threshold.
    """
    # Check if we're over the storage limit
    total_size = sum(get_folder_size(folder) for folder in [UPLOAD_FOLDER, EXTRACTED_FOLDER, PROCESSED_FOLDER])
    
    if total_size > MAX_STORAGE_SIZE:
        logger.warning(f"Storage usage ({round(total_size / (1024 * 1024 * 1024), 2)} GB) exceeds limit ({round(MAX_STORAGE_SIZE / (1024 * 1024 * 1024), 2)} GB). Starting cleanup...")
        
        # First try removing old files
        removed_count, removed_size = cleanup_old_files(UPLOAD_FOLDER)
        if removed_count == 0:
            removed_count, removed_size = cleanup_old_files(EXTRACTED_FOLDER)
            removed_count, removed_size = cleanup_old_files(PROCESSED_FOLDER)
        
        # If still over limit, remove oldest files regardless of age
        total_size = sum(get_folder_size(folder) for folder in [UPLOAD_FOLDER, EXTRACTED_FOLDER, PROCESSED_FOLDER])
        if total_size > MAX_STORAGE_SIZE:
            logger.warning(f"Still over storage limit. Removing oldest files...")
            
            # Get all files with their modification times
            all_files = []
            for folder in [PROCESSED_FOLDER, EXTRACTED_FOLDER, UPLOAD_FOLDER]:  # Process PROCESSED_FOLDER first
                for dirpath, _, filenames in os.walk(folder):
                    for filename in filenames:
                        file_path = os.path.join(dirpath, filename)
                        if os.path.isfile(file_path):
                            all_files.append((file_path, os.path.getmtime(file_path)))
            
            # Sort by modification time (oldest first)
            all_files.sort(key=lambda x: x[1])
            
            # Remove oldest files until under limit
            for file_path, _ in all_files:
                if total_size <= MAX_STORAGE_SIZE * 0.9:  # Remove until 90% of limit
                    break
                    
                try:
                    file_size = os.path.getsize(file_path)
                    os.remove(file_path)
                    total_size -= file_size
                    logger.info(f"Removed oldest file: {file_path} ({round(file_size / (1024 * 1024), 2)} MB)")
                except Exception as e:
                    logger.warning(f"Failed to remove file {file_path}: {e}")
        
        logger.info(f"Cleanup complete. Current storage usage: {round(total_size / (1024 * 1024 * 1024), 2)} GB")