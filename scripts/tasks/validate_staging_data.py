"""
Validate and Ingest Staging Data Task

This is the main task that validates data BEFORE inserting into the staging table.
Valid records go to the raw staging table, invalid ones go straight to quarantine.
This way I never have bad data sitting in my staging table.

The heavy lifting is done by the utility modules, keeping this file clean and focused.
"""

import sys
sys.path.append('/opt/airflow/scripts')

import os
import logging
from pathlib import Path
from airflow.exceptions import AirflowSkipException, AirflowException
from dotenv import load_dotenv

# Importing my modular utilities
from utils.dataset_tracker import get_unprocessed_datasets, mark_as_processed
from utils.csv_reader import read_flight_price_csv
from utils.data_validator import validate_dataframe
from utils.db_operations import insert_to_staging, insert_to_quarantine

load_dotenv()

logger = logging.getLogger(__name__)

# Setting a base path for my data directory so I can easily change it later if needed
DATA_BASE_PATH = Path(os.environ.get('AIRFLOW_DATA_PATH', '/opt/airflow/data'))

# If more than 90% of records are invalid, something is seriously wrong
CATASTROPHIC_THRESHOLD = 0.90


def validate_staging_data(**context):
    """
    Main orchestration function that coordinates the validate-first pipeline.
    
    Flow:
    1. Find unprocessed datasets
    2. Read and preprocess the CSV
    3. Validate all records in memory
    4. Insert valid records to staging, invalid to quarantine
    5. Mark file as processed and push metrics
    """
    # First, let me find a dataset to process
    csv_path, processed_files = get_unprocessed_datasets(DATA_BASE_PATH)
    
    if csv_path is None:
        raise AirflowSkipException("No unprocessed datasets found. Skipping this run.")
    
    logger.info(f"Starting validation for: {csv_path.name}")
    
    # Reading and preprocessing the CSV file
    df = read_flight_price_csv(csv_path)
    total_rows = len(df)
    
    logger.info(f"Loaded {total_rows:,} rows. Now running validation...")
    
    # Running validation on the dataframe BEFORE inserting anything
    valid_df, invalid_df = validate_dataframe(df)
    
    valid_count = len(valid_df)
    invalid_count = len(invalid_df)
    invalid_pct = round(invalid_count / total_rows * 100, 2) if total_rows > 0 else 0
    
    logger.info(f"Validation complete. Valid: {valid_count:,}, Invalid: {invalid_count:,} ({invalid_pct}%)")
    
    # Handling invalid records first by sending them to quarantine
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count:,} invalid records, sending them to quarantine...")
        insert_to_quarantine(invalid_df)
    
    # Now inserting the valid records into the staging table
    if valid_count > 0:
        logger.info(f"Inserting {valid_count:,} valid records into staging table...")
        # Excluding quarantine-specific columns that shouldn't go into staging
        insert_to_staging(
            valid_df, 
            exclude_columns=['quarantine_timestamp', 'quarantine_reason_summary']
        )
    
    # Marking this file as processed so I don't process it again
    mark_as_processed(csv_path.name, processed_files)
    
    # Pushing metrics to XCom so downstream tasks can use them
    context['ti'].xcom_push(key='validation_summary', value={
        'file_name': csv_path.name,
        'total_rows': total_rows,
        'valid_rows': valid_count,
        'invalid_count': invalid_count,
        'invalid_pct': invalid_pct,
        'quarantined': invalid_count > 0
    })
    
    # If almost everything is bad, something is seriously wrong with the source data
    if invalid_pct > CATASTROPHIC_THRESHOLD * 100:
        raise AirflowException(
            f"CRITICAL: {invalid_pct}% of records invalid! "
            f"Pipeline stopped to prevent processing completely broken data. "
            f"Check source file and ingestion logic."
        )
    
    # Logging the final status
    if invalid_count == 0:
        logger.info("All records passed validation - nice!")
    else:
        logger.warning(
            f"Pipeline continues with {valid_count:,} valid records. "
            f"Check quarantine table for the {invalid_count:,} problematic ones."
        )