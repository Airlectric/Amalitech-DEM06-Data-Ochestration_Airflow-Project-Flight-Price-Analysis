"""
Dataset Tracker Utility

This module handles tracking which datasets have been processed.
I use Airflow Variables to store the list of processed files so I don't re-process them.
"""

import json
import logging
from pathlib import Path
from airflow.models import Variable

logger = logging.getLogger(__name__)


def get_processed_files() -> list:
    """
    Gets the list of files I've already processed from Airflow Variables.
    Returns an empty list if nothing has been processed yet.
    """
    try:
        return json.loads(Variable.get('processed_flight_datasets', default_var='[]'))
    except Exception:
        return []


def mark_as_processed(file_name: str, processed_files: list) -> None:
    """
    After I successfully process a file, this marks it so I don't process it again.
    """
    if file_name not in processed_files:
        processed_files.append(file_name)
        Variable.set('processed_flight_datasets', json.dumps(processed_files))
        logger.info(f"Marked '{file_name}' as processed")


def get_unprocessed_datasets(data_path: Path) -> tuple:
    """
    Scans the data folder and finds CSV files that haven't been processed yet.
    Returns the most recent unprocessed file based on modification time.
    
    Args:
        data_path: Path to the data directory
        
    Returns:
        Tuple of (most_recent_file_path or None, list of processed_files)
    """
    processed_files = get_processed_files()
    
    # Finding all CSV files in the data directory
    csv_files = list(data_path.glob('*.csv'))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {data_path}")
        return None, processed_files
    
    # Filtering out the ones I've already processed
    unprocessed = [f for f in csv_files if f.name not in processed_files]
    
    if not unprocessed:
        logger.info("All datasets have been processed already. Nothing new to ingest.")
        return None, processed_files
    
    # Sorting by modification time so I get the most recent one first
    unprocessed.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    most_recent = unprocessed[0]
    
    logger.info(f"Found {len(unprocessed)} unprocessed file(s). Picking most recent: {most_recent.name}")
    return most_recent, processed_files
