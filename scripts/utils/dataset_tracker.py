"""
Dataset Tracker Utility

This module handles tracking which datasets have been processed.
Instead of just marking files as processed, I move them to an archived folder.
This is better because if a new file comes in with the same name, it won't be confused with the old one.
"""

import shutil
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

# Name of the folder where processed files go
ARCHIVE_FOLDER_NAME = 'archived'


def ensure_archive_folder(data_path: Path) -> Path:
    """
    Makes sure the archived folder exists inside the data directory.
    Creates it if it doesn't exist.
    """
    archive_path = data_path / ARCHIVE_FOLDER_NAME
    archive_path.mkdir(parents=True, exist_ok=True)
    return archive_path


def archive_processed_file(file_path: Path) -> Path:
    """
    Moves a processed file to the archived folder.
    Adds a timestamp to the filename to avoid conflicts if a file with the same name is processed again later.
    
    Args:
        file_path: Path to the file to archive
        
    Returns:
        Path to the archived file
    """
    if not file_path.exists():
        logger.warning(f"File not found, can't archive: {file_path}")
        return None
    
    # Getting the archive folder (creating it if needed)
    archive_path = ensure_archive_folder(file_path.parent)
    
    # Adding timestamp to filename to make it unique
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archived_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
    archived_file_path = archive_path / archived_name
    
    # Moving the file
    shutil.move(str(file_path), str(archived_file_path))
    logger.info(f"Archived '{file_path.name}' to '{archived_file_path}'")
    
    return archived_file_path


def get_unprocessed_datasets(data_path: Path) -> tuple:
    """
    Scans the data folder and finds CSV files that haven't been processed yet.
    Only looks at files in the main data directory, not in the archived folder.
    Returns the most recent unprocessed file based on modification time.
    
    Args:
        data_path: Path to the data directory
        
    Returns:
        Tuple of (most_recent_file_path or None, archive_path)
    """
    # Making sure the archive folder exists
    archive_path = ensure_archive_folder(data_path)
    
    # Finding all CSV files in the main data directory (not in archived subfolder)
    csv_files = [f for f in data_path.glob('*.csv') if f.is_file()]
    
    if not csv_files:
        logger.info(f"No CSV files found in {data_path} (excluding archived folder)")
        return None, archive_path
    
    # Sorting by modification time so I get the most recent one first
    csv_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    most_recent = csv_files[0]
    
    logger.info(f"Found {len(csv_files)} unprocessed file(s). Picking most recent: {most_recent.name}")
    return most_recent, archive_path


# Keeping these for backwards compatibility, but they're not really needed anymore
def get_processed_files() -> list:
    """Legacy function - not needed when using archive approach."""
    return []


def mark_as_processed(file_name: str, processed_files: list) -> None:
    """Legacy function - use archive_processed_file instead."""
    logger.info(f"Note: mark_as_processed is deprecated. Use archive_processed_file instead.")
